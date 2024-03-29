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
    def __init__(self, toIp, toPort, streamName, sampRate, verbose=False):
        self.toIp = toIp
        self.toPort = toPort
        self.streamName = streamName
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
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

    def _constructFrame(self, pcmData):
        header = b"VBAN"
        header += bytes([self.const_VBAN_SR.index(self.samprate)])
        header += bytes([self.chunkSize-1])
        header += bytes([self.channels-1])
        header += b'\x01'
        header += bytes(self.streamName + "\x00" *
                        (16 - len(self.streamName)), 'utf-8')
        header += struct.pack("<L", self.framecounter)
        if self.verbose:
            logging.debug("SVBAN "+str(self.samprate)+"Hz "+str(self.chunkSize)+"samp "+str(self.channels) +
                          "chan Format:1 Name:"+self.streamName+" Frame:"+str(self.framecounter))
            print("SVBAN "+str(self.samprate)+"Hz "+str(self.chunkSize)+"samp "+str(self.channels) +
                  "chan Format:1 Name:"+self.streamName+" Frame:"+str(self.framecounter))
        return header + pcmData

    def runonce(self, pcmData):
        try:
            self.framecounter += 1
            self.last_active_time = time.time()
            self.rawPcm = pcmData
            self.rawData = self._constructFrame(self.rawPcm)
            self.sock.sendto(self.rawData, (self.toIp, self.toPort))
        except Exception as e:
            print(e)

    def _ffmpeg_thread(self, process):
        print("Thread started.")  # For debugging
        while True:
            chunk = process.stdout.read(self.chunkSize * self.channels * 2)
            if not chunk:
                print("No chunk read. Terminating script.")  # For debugging
                os._exit(1)
            try:
                self.runonce(chunk)
            except Exception as e:
                print("Error sending data:", e)
                os._exit(1)

    def runforever(self, url):
        cmd = ["ffmpeg", "-hide_banner", "-re", "-i", url, "-f", "s16le", "-acodec", "pcm_s16le",
               "-ar", "48000", "-ac", "2", "-loglevel", "warning", "-"]
        process = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=4096)

        ffmpeg_thread = threading.Thread(
            target=self._ffmpeg_thread, args=(process,))

        error_log_thread = threading.Thread(
            target=self._log_errors, args=(process,))

        ffmpeg_thread.start()
        error_log_thread.start()

        ffmpeg_thread.join()
        error_log_thread.join()

    def _log_errors(self, process):
        for line in process.stderr:
            print(f"{line.decode().rstrip()}")


def monitor_frame_counter(sender):
    while True:
        time.sleep(5)  # Check every 5 seconds
        if sender.last_active_time is not None and time.time() - sender.last_active_time > 10:
            print("Frame counter inactive for more than 10 seconds. Terminating.")
            os._exit(1)


if __name__ == "__main__":
    # Read from the config file
    config = configparser.ConfigParser()
    config.read('config.ini')

    input_url = config['input']['url']
    vban_ip = config['vban']['ip']
    vban_port = int(config['vban']['port'])
    vban_name = config['vban']['name']

    sender = VBAN_Send(vban_ip, vban_port, vban_name, 48000, verbose=True)

    monitor_thread = threading.Thread(
        target=monitor_frame_counter, args=(sender,))
    monitor_thread.start()

    sender.runforever(input_url)
