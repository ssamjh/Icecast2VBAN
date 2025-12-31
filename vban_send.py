import socket
import struct
import subprocess
import threading
import configparser
import logging
import time
import os
from logging.handlers import RotatingFileHandler


class VBAN_Send(object):
    def __init__(self, toIp, toPort, streamName, sampRate, config, verbose=False):
        self.toIp = toIp
        self.toPort = toPort
        self.streamName = streamName
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        # Read performance parameters from config
        socket_timeout = float(config.get('performance', 'socket_timeout', fallback='5.0'))
        self.sock.settimeout(socket_timeout)
        self.const_VBAN_SR = [6000, 12000, 24000, 48000, 96000, 192000, 384000, 8000, 16000,
                              32000, 64000, 128000, 256000, 512000, 11025, 22050, 44100, 88200, 176400, 352800, 705600]
        if sampRate not in self.const_VBAN_SR:
            logging.error("SampRate not valid/compatible")
            print("SampRate not valid/compatible")
            return
        self.samprate = sampRate
        self.chunkSize = 256
        self.channels = 2
        self.framecounter = 0
        self.verbose = verbose
        self.rawPcm = None
        self.rawData = None
        self.last_active_time = None

        # Graceful shutdown event
        self.running = threading.Event()
        self.running.set()

    def _constructFrame(self, pcmData):
        header = b"VBAN"
        header += bytes([self.const_VBAN_SR.index(self.samprate)])
        header += bytes([self.chunkSize-1])
        header += bytes([self.channels-1])
        header += b'\x01'
        header += bytes(self.streamName + "\x00" *
                        (16 - len(self.streamName)), 'utf-8')
        header += struct.pack("<L", self.framecounter)
        return header + pcmData

    def runonce(self, pcmData):
        try:
            self.framecounter += 1
            self.last_active_time = time.time()
            self.rawPcm = pcmData
            self.rawData = self._constructFrame(self.rawPcm)
            self.sock.sendto(self.rawData, (self.toIp, self.toPort))
            self._update_healthcheck()
        except Exception as e:
            print(e)

    def _update_healthcheck(self):
        """Update healthcheck file for Docker HEALTHCHECK"""
        try:
            with open('/tmp/vban_healthy', 'w') as f:
                f.write(str(int(time.time())))
        except:
            pass  # Don't fail if healthcheck file can't be written

    def _ffmpeg_thread(self, process):
        """Read from FFmpeg and send packets directly"""
        while self.running.is_set():
            chunk = process.stdout.read(self.chunkSize * self.channels * 2)
            if not chunk:
                logging.error("FFmpeg stream ended")
                self.running.clear()
                break

            try:
                self.framecounter += 1
                self.last_active_time = time.time()
                rawData = self._constructFrame(chunk)
                try:
                    self.sock.sendto(rawData, (self.toIp, self.toPort))
                except (ConnectionRefusedError, OSError) as e:
                    logging.warning(f"Socket error (ignoring): {e}")
                    # UDP is connectionless, these errors are non-fatal
                self._update_healthcheck()
            except Exception as e:
                logging.error(f"Error sending data: {e}")
                self.running.clear()
                break

    def runforever(self, url):
        cmd = ["ffmpeg", "-hide_banner", "-re", "-i", url,
               "-f", "s16le", "-acodec", "pcm_s16le",
               "-ar", "48000", "-ac", "2",
               "-loglevel", "warning", "-"]

        process = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            bufsize=4096)

        self.process = process  # Store for cleanup

        ffmpeg_thread = threading.Thread(
            target=self._ffmpeg_thread, args=(process,))
        ffmpeg_thread.daemon = True

        error_log_thread = threading.Thread(
            target=self._log_errors, args=(process,))
        error_log_thread.daemon = True

        ffmpeg_thread.start()
        error_log_thread.start()

        # Wait for thread to complete or shutdown signal
        ffmpeg_thread.join()

        # Ensure FFmpeg process is terminated
        if process.poll() is None:
            logging.info("Terminating FFmpeg process")
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                logging.warning("FFmpeg didn't terminate, killing")
                process.kill()
                process.wait()

        error_log_thread.join(timeout=1)

    def _log_errors(self, process):
        for line in process.stderr:
            line_str = line.decode().rstrip()
            if line_str:  # Only log non-empty lines
                logging.error(f"FFmpeg: {line_str}")


def monitor_frame_counter(sender):
    """Monitor frame counter and trigger graceful shutdown if inactive"""
    while sender.running.is_set():
        time.sleep(5)  # Check every 5 seconds
        if sender.last_active_time is not None and time.time() - sender.last_active_time > 10:
            logging.error("Frame counter inactive for more than 10 seconds")
            sender.running.clear()  # Signal graceful shutdown
            break


if __name__ == "__main__":
    # Read from the config file
    config = configparser.ConfigParser()
    config.read('config.ini')

    input_url = config['input']['url']
    vban_ip = config['vban']['ip']
    vban_port = int(config['vban']['port'])
    vban_name = config['vban']['name']

    # Read retry parameters
    retry_delay = int(config.get('performance', 'retry_delay', fallback='1'))
    max_retry_delay = int(config.get('performance', 'max_retry_delay', fallback='60'))
    current_retry_delay = retry_delay

    # Graceful recovery loop with exponential backoff
    while True:
        try:
            sender = VBAN_Send(vban_ip, vban_port, vban_name, 48000, config, verbose=True)

            monitor_thread = threading.Thread(
                target=monitor_frame_counter, args=(sender,))
            monitor_thread.daemon = True
            monitor_thread.start()

            logging.info(f"Starting VBAN stream to {vban_ip}:{vban_port}")
            sender.runforever(input_url)

            # If we get here, stream ended cleanly
            logging.warning(f"Stream ended, retrying in {current_retry_delay}s")
            time.sleep(current_retry_delay)
            current_retry_delay = min(current_retry_delay * 2, max_retry_delay)

        except Exception as e:
            logging.error(f"Error in VBAN stream: {e}")
            logging.warning(f"Retrying in {current_retry_delay}s")
            time.sleep(current_retry_delay)
            current_retry_delay = min(current_retry_delay * 2, max_retry_delay)
