import socket
import struct
import subprocess
import threading
import configparser
import queue


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
            print("SampRate not valid/compatible")
            return
        self.samprate = sampRate
        self.chunkSize = 256
        self.channels = 2  # Assuming stereo for simplicity
        self.framecounter = 0
        self.verbose = verbose
        self.rawPcm = None
        self.rawData = None

    def _constructFrame(self, pcmData):
        header = b"VBAN"
        header += bytes([self.const_VBAN_SR.index(self.samprate)])
        header += bytes([self.chunkSize-1])
        header += bytes([self.channels-1])
        header += b'\x01'  # VBAN_CODEC_PCM
        header += bytes(self.streamName + "\x00" *
                        (16 - len(self.streamName)), 'utf-8')
        header += struct.pack("<L", self.framecounter)
        if self.verbose:
            print("SVBAN "+str(self.samprate)+"Hz "+str(self.chunkSize)+"samp "+str(self.channels) +
                  "chan Format:1 Name:"+self.streamName+" Frame:"+str(self.framecounter))
        return header + pcmData

    def runonce(self, pcmData):
        try:
            self.framecounter += 1
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
                print("No chunk read. Thread terminating.")  # For debugging
                break
            try:
                self.runonce(chunk)
            except Exception as e:
                print("Error sending data:", e)
                break
        err = process.stderr.read().decode('utf-8')
        if err:
            print("FFmpeg Error:", err)

    def runforever(self, url):
        cmd = ["ffmpeg", "-re", "-i", url, "-f", "s16le", "-acodec", "pcm_s16le",
               "-ar", "48000", "-ac", "2", "-bufsize", "1280K", "-"]
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
            print(f"FFMPEG Error: {line.decode().rstrip()}")



if __name__ == "__main__":
    # Read from the config file
    config = configparser.ConfigParser()
    config.read('config.ini')

    input_url = config['input']['url']
    vban_ip = config['vban']['ip']
    vban_port = int(config['vban']['port'])
    vban_name = config['vban']['name']

    sender = VBAN_Send(vban_ip, vban_port, vban_name,
                       48000, verbose=True)
    sender.runforever(input_url)
