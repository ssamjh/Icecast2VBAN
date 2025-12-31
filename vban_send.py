import socket
import struct
import subprocess
import threading
import configparser
import logging
import time
import os
import queue
from logging.handlers import RotatingFileHandler


class VBAN_Send(object):
    def __init__(self, toIp, toPort, streamName, sampRate, config, verbose=False):
        self.toIp = toIp
        self.toPort = toPort
        self.streamName = streamName
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        # Read performance parameters from config
        socket_timeout = float(config.get('performance', 'socket_timeout', fallback='0.1'))
        self.queue_size = int(config.get('performance', 'queue_size', fallback='20'))
        self.healthcheck_interval = int(config.get('performance', 'healthcheck_interval', fallback='50'))
        self.pipe_buffer_size = int(config.get('performance', 'pipe_buffer_size', fallback='262144'))

        self.sock.settimeout(socket_timeout)
        self.sock.connect((self.toIp, self.toPort))
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

        # Intermediate packet queue for buffering
        self.packet_queue = queue.Queue(maxsize=self.queue_size)

        # Graceful shutdown event
        self.running = threading.Event()
        self.running.set()

        # Healthcheck throttling counter
        self.healthcheck_counter = 0

        # Statistics tracking
        self.stats = {
            'packets_sent': 0,
            'packets_dropped': 0,
            'queue_full_count': 0,
            'queue_empty_count': 0
        }

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
        """Update healthcheck file for Docker HEALTHCHECK (throttled)"""
        self.healthcheck_counter += 1
        if self.healthcheck_counter >= self.healthcheck_interval:
            self.healthcheck_counter = 0
            try:
                with open('/tmp/vban_healthy', 'w') as f:
                    f.write(str(int(time.time())))
            except:
                pass  # Don't fail if healthcheck file can't be written

    def _ffmpeg_read_thread(self, process):
        """Fast read thread - only reads from FFmpeg and queues packets"""
        while self.running.is_set():
            chunk = process.stdout.read(self.chunkSize * self.channels * 2)
            if not chunk:
                logging.error("FFmpeg stream ended")
                self.running.clear()
                break
            try:
                self.packet_queue.put(chunk, timeout=1.0)
            except queue.Full:
                logging.warning("Packet queue full, dropping packet")
                self.stats['queue_full_count'] += 1
                self.stats['packets_dropped'] += 1

    def _vban_send_thread(self):
        """Timed send thread - sends packets at precise intervals"""
        packet_interval = self.chunkSize / self.samprate  # 256/48000 = 5.33ms
        next_send_time = time.perf_counter()

        while self.running.is_set():
            try:
                chunk = self.packet_queue.get(timeout=1.0)

                # Send packet
                self.framecounter += 1
                self.last_active_time = time.time()
                rawData = self._constructFrame(chunk)
                self.sock.sendto(rawData, (self.toIp, self.toPort))
                self._update_healthcheck()
                self.stats['packets_sent'] += 1

                # Precise timing for next packet
                next_send_time += packet_interval
                sleep_time = next_send_time - time.perf_counter()
                if sleep_time > 0:
                    time.sleep(sleep_time)
                else:
                    # Fell behind, reset timing
                    next_send_time = time.perf_counter()

            except queue.Empty:
                logging.warning("Packet queue empty - stream underrun")
                self.stats['queue_empty_count'] += 1

    def runforever(self, url):
        # Optimized FFmpeg command for low latency
        cmd = ["ffmpeg", "-hide_banner",
               "-analyzeduration", "0", "-probesize", "32768",
               "-fflags", "+nobuffer+flush_packets", "-flags", "low_delay",
               "-i", url,
               "-f", "s16le", "-acodec", "pcm_s16le",
               "-ar", "48000", "-ac", "2",
               "-loglevel", "warning", "-"]

        process = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            bufsize=self.pipe_buffer_size)

        # Separate read and send threads for pipeline parallelism
        read_thread = threading.Thread(
            target=self._ffmpeg_read_thread, args=(process,))
        read_thread.daemon = True

        send_thread = threading.Thread(target=self._vban_send_thread)
        send_thread.daemon = True

        error_log_thread = threading.Thread(
            target=self._log_errors, args=(process,))
        error_log_thread.daemon = True

        read_thread.start()
        send_thread.start()
        error_log_thread.start()

        read_thread.join()
        send_thread.join()
        error_log_thread.join()

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
