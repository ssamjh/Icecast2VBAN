import socket
import struct
import subprocess
import threading
import configparser
import logging
import time
from collections import deque
import statistics
import queue


class DebugStats:
    """Tracks timing and performance metrics for audio streaming debugging"""

    def __init__(self, enabled=False, report_interval=10):
        self.enabled = enabled
        self.report_interval = report_interval

        # Timing metrics
        self.last_packet_time = None
        self.packet_intervals = deque(maxlen=1000)  # Last 1000 packet intervals
        self.ffmpeg_read_times = deque(maxlen=1000)
        self.send_times = deque(maxlen=1000)

        # Packet metrics
        self.packets_sent = 0
        self.packets_dropped = 0
        self.total_bytes_sent = 0
        self.short_chunks = 0  # Chunks smaller than expected

        # Timing anomalies
        self.gaps_detected = 0  # Intervals > 10ms
        self.fast_packets = 0   # Intervals < 3ms

        # Queue metrics
        self.queue_depths = deque(maxlen=1000)
        self.queue_full_events = 0
        self.buffer_underruns = 0

        # Timing accuracy metrics
        self.timing_corrections = 0

        # Throttling metrics
        self.throttle_events = 0
        self.throttle_time_total = 0.0  # Total ms spent throttling
        self.queue_growth_events = 0
        self.queue_shrink_events = 0

        # Stream info
        self.stream_type = "unknown"
        self.stream_url = ""

        # Last report time
        self.last_report_time = time.time()
        self.start_time = time.time()

    def detect_stream_type(self, url):
        """Detect if stream is HLS or Icecast based on URL"""
        url_lower = url.lower()
        if '.m3u8' in url_lower or '/hls/' in url_lower:
            self.stream_type = "HLS"
        elif 'icecast' in url_lower or ':8000/' in url_lower:
            self.stream_type = "Icecast"
        elif url_lower.startswith('http'):
            self.stream_type = "HTTP Stream"
        else:
            self.stream_type = "Unknown"
        self.stream_url = url

        if self.enabled:
            logging.info(f"[DEBUG] Detected stream type: {self.stream_type}")

    def record_packet(self, chunk_size, read_time, send_time, queue_depth=None):
        """Record metrics for a single packet"""
        if not self.enabled:
            return

        current_time = time.time()

        # Record packet interval
        if self.last_packet_time is not None:
            interval = (current_time - self.last_packet_time) * 1000  # Convert to ms
            self.packet_intervals.append(interval)

            # Detect anomalies (expected: ~5.33ms at 48kHz, 256 samples)
            if interval > 10:
                self.gaps_detected += 1
            elif interval < 3:
                self.fast_packets += 1

        self.last_packet_time = current_time

        # Record timing
        self.ffmpeg_read_times.append(read_time * 1000)  # Convert to ms
        self.send_times.append(send_time * 1000)

        # Record packet info
        self.packets_sent += 1
        self.total_bytes_sent += chunk_size

        # Record queue depth if provided
        if queue_depth is not None:
            self.queue_depths.append(queue_depth)

        # Detect short chunks (expected: 1024 bytes for stereo s16le)
        if chunk_size < 1024:
            self.short_chunks += 1

    def record_queue_full(self):
        """Record when queue becomes full"""
        if self.enabled:
            self.queue_full_events += 1

    def record_buffer_underrun(self):
        """Record when queue becomes empty during playback"""
        if self.enabled:
            self.buffer_underruns += 1

    def record_timing_correction(self):
        """Record when timing had to be resynced"""
        if self.enabled:
            self.timing_corrections += 1

    def record_drop(self):
        """Record a dropped packet"""
        if self.enabled:
            self.packets_dropped += 1

    def record_throttle(self, sleep_time):
        """Record throttling event and sleep time"""
        if self.enabled:
            self.throttle_events += 1
            self.throttle_time_total += sleep_time * 1000  # Convert to ms

    def record_queue_growth(self):
        """Record queue expansion event"""
        if self.enabled:
            self.queue_growth_events += 1

    def record_queue_shrink(self):
        """Record queue shrink event"""
        if self.enabled:
            self.queue_shrink_events += 1

    def should_report(self):
        """Check if it's time to generate a report"""
        if not self.enabled:
            return False
        return (time.time() - self.last_report_time) >= self.report_interval

    def generate_report(self):
        """Generate and log performance statistics"""
        if not self.enabled or len(self.packet_intervals) == 0:
            return

        elapsed = time.time() - self.start_time

        # Calculate statistics
        avg_interval = statistics.mean(self.packet_intervals)
        median_interval = statistics.median(self.packet_intervals)
        stdev_interval = statistics.stdev(self.packet_intervals) if len(self.packet_intervals) > 1 else 0
        min_interval = min(self.packet_intervals)
        max_interval = max(self.packet_intervals)

        avg_read = statistics.mean(self.ffmpeg_read_times) if self.ffmpeg_read_times else 0
        avg_send = statistics.mean(self.send_times) if self.send_times else 0

        # Calculate jitter (variation in packet timing)
        jitter_ms = stdev_interval

        # Expected interval for 256 samples at 48kHz: 5.333ms
        expected_interval = 5.333
        timing_drift = avg_interval - expected_interval

        logging.info("=" * 80)
        logging.info(f"[DEBUG STATS] Audio Stream Performance Report")
        logging.info(f"Stream Type: {self.stream_type} | URL: {self.stream_url[:60]}...")
        logging.info(f"Runtime: {elapsed:.1f}s | Packets: {self.packets_sent} | Drops: {self.packets_dropped}")
        logging.info(f"Data Sent: {self.total_bytes_sent / 1024 / 1024:.2f} MB")
        logging.info("-" * 80)
        logging.info(f"Packet Timing (ms):")
        logging.info(f"  Expected: {expected_interval:.3f} | Actual Avg: {avg_interval:.3f} | Median: {median_interval:.3f}")
        logging.info(f"  Min: {min_interval:.3f} | Max: {max_interval:.3f} | Std Dev: {stdev_interval:.3f}")
        logging.info(f"  Jitter: {jitter_ms:.3f}ms | Drift: {timing_drift:+.3f}ms")
        logging.info("-" * 80)
        logging.info(f"Timing Breakdown (ms):")
        logging.info(f"  FFmpeg Read: {avg_read:.3f} | Network Send: {avg_send:.3f}")
        logging.info("-" * 80)
        logging.info(f"Anomalies Detected:")
        logging.info(f"  Large Gaps (>10ms): {self.gaps_detected} ({self.gaps_detected/self.packets_sent*100:.2f}%)")
        logging.info(f"  Fast Packets (<3ms): {self.fast_packets} ({self.fast_packets/self.packets_sent*100:.2f}%)")
        logging.info(f"  Short Chunks: {self.short_chunks}")

        # Queue statistics if available
        if self.queue_depths:
            avg_queue = statistics.mean(self.queue_depths)
            max_queue = max(self.queue_depths)
            min_queue = min(self.queue_depths)
            logging.info("-" * 80)
            logging.info(f"Dejitter Buffer Statistics:")
            logging.info(f"  Queue Depth: Avg {avg_queue:.1f} | Min {min_queue} | Max {max_queue}")
            logging.info(f"  Buffer Full Events: {self.queue_full_events} | Underruns: {self.buffer_underruns}")
            logging.info(f"  Timing Corrections: {self.timing_corrections}")

            # Calculate buffer health percentage
            buffer_health = (1 - (self.buffer_underruns / max(self.packets_sent, 1))) * 100
            logging.info(f"  Buffer Health: {buffer_health:.2f}% (higher is better)")

        # Throttling statistics if throttling was active
        if self.throttle_events > 0:
            runtime_ms = elapsed * 1000
            avg_throttle = self.throttle_time_total / self.throttle_events
            throttle_pct = (self.throttle_time_total / runtime_ms) * 100
            logging.info("-" * 80)
            logging.info(f"Throttling Statistics:")
            logging.info(f"  Active: {throttle_pct:.1f}% | Events: {self.throttle_events}")
            logging.info(f"  Avg Sleep: {avg_throttle:.3f}ms")
            if self.queue_growth_events > 0 or self.queue_shrink_events > 0:
                logging.info(f"  Queue Resizes: {self.queue_growth_events} grew, {self.queue_shrink_events} shrunk")

        logging.info("=" * 80)

        # Reset counters for next interval
        self.last_report_time = time.time()
        self.gaps_detected = 0
        self.fast_packets = 0


class VBAN_Send(object):
    def __init__(self, toIp, toPort, streamName, sampRate, config):
        self.toIp = toIp
        self.toPort = toPort
        self.streamName = streamName
        self.config = config  # Store config for later use
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
        self.last_active_time = None

        # Initialize debug stats
        debug_enabled = config.getboolean('debug', 'debug_audio', fallback=False)
        debug_interval = float(config.get('debug', 'debug_report_interval', fallback='10'))
        self.debug_stats = DebugStats(enabled=debug_enabled, report_interval=debug_interval)

        if debug_enabled:
            logging.info("[DEBUG] Audio debugging enabled")

        # Packet queue for buffering between FFmpeg and network send
        queue_size = int(config.get('performance', 'queue_size', fallback='20'))
        self.packet_queue = queue.Queue(maxsize=queue_size)

        if debug_enabled:
            logging.info(f"[DEBUG] Packet queue size: {queue_size} packets (~{queue_size * 5.33:.1f}ms buffering)")

        # Adaptive throttling configuration
        self.enable_throttling = config.getboolean('performance', 'enable_adaptive_throttling', fallback=True)
        self.throttle_threshold = config.getfloat('performance', 'throttle_threshold', fallback=0.70)
        self.target_min = config.getfloat('performance', 'target_queue_min', fallback=0.75)
        self.target_max = config.getfloat('performance', 'target_queue_max', fallback=0.90)

        # Dynamic queue growth configuration
        self.enable_queue_growth = config.getboolean('performance', 'enable_queue_growth', fallback=True)
        self.max_queue_size = int(config.get('performance', 'max_queue_size', fallback='200'))
        self.growth_increment = int(config.get('performance', 'growth_increment', fallback='20'))
        self.initial_queue_size = queue_size  # Remember initial size
        self.queue_lock = threading.Lock()  # For thread-safe queue resizing

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

    def _update_healthcheck(self):
        """Update healthcheck file for Docker HEALTHCHECK"""
        try:
            with open('/tmp/vban_healthy', 'w') as f:
                f.write(str(int(time.time())))
        except:
            pass  # Don't fail if healthcheck file can't be written

    def _resize_queue(self, new_size):
        """Resize packet queue by creating new queue and transferring packets"""
        with self.queue_lock:  # Prevent race conditions
            old_queue = self.packet_queue
            new_queue = queue.Queue(maxsize=new_size)

            # Transfer existing packets
            while not old_queue.empty():
                try:
                    item = old_queue.get_nowait()
                    new_queue.put_nowait(item)
                except:
                    break

            self.packet_queue = new_queue

            if self.debug_stats.enabled:
                logging.info(f"[RESIZE] Queue resized: {old_queue.maxsize} → {new_size} packets ({new_size * 5.33:.1f}ms buffer)")

    def _ffmpeg_thread(self, process):
        """Read from FFmpeg and enqueue packets"""
        while self.running.is_set():
            # Measure FFmpeg read time
            read_start = time.time()
            chunk = process.stdout.read(self.chunkSize * self.channels * 2)
            read_time = time.time() - read_start

            if not chunk:
                logging.error("FFmpeg stream ended")
                self.running.clear()
                break

            # Log if read took unusually long
            if self.debug_stats.enabled and read_time > 0.010:  # > 10ms
                logging.warning(f"[DEBUG] Slow FFmpeg read: {read_time*1000:.2f}ms (expected ~5ms)")

            # Log short chunks
            if self.debug_stats.enabled and len(chunk) < (self.chunkSize * self.channels * 2):
                logging.warning(f"[DEBUG] Short chunk: {len(chunk)} bytes (expected {self.chunkSize * self.channels * 2})")

            try:
                # Try to enqueue the packet (non-blocking with timeout)
                try:
                    self.packet_queue.put((chunk, read_time), timeout=0.1)
                except queue.Full:
                    if self.debug_stats.enabled:
                        logging.warning("[DEBUG] Packet queue full, attempting resize")

                    # Try to grow queue if enabled
                    if self.enable_queue_growth:
                        current_size = self.packet_queue.maxsize
                        if current_size < self.max_queue_size:
                            new_size = min(current_size + self.growth_increment, self.max_queue_size)
                            self._resize_queue(new_size)

                            # Retry enqueue with new larger queue
                            try:
                                self.packet_queue.put((chunk, read_time), timeout=0.1)
                                self.debug_stats.record_queue_growth()
                                # Success - skip to throttling check below
                            except queue.Full:
                                pass  # Still full, fall through to drop

                    # Fallback: drop packet
                    if self.packet_queue.qsize() >= self.packet_queue.maxsize:
                        logging.warning("[DROP] Packet queue full despite resize")
                        self.debug_stats.record_queue_full()
                        self.debug_stats.record_drop()

            except Exception as e:
                logging.error(f"Error enqueueing packet: {e}")
                self.running.clear()
                break

            # ADAPTIVE THROTTLING
            if self.enable_throttling:
                current_depth = self.packet_queue.qsize()
                max_size = self.packet_queue.maxsize
                fullness = current_depth / max_size if max_size > 0 else 0

                # Calculate target interval for this audio format
                target_interval = self.chunkSize / self.samprate  # 5.333ms for 256@48kHz

                # Throttle if queue is filling up
                if fullness >= self.throttle_threshold:
                    # Calculate throttle factor (0.0 to 1.0)
                    # At threshold (70%): factor = 0.0 (no sleep)
                    # At 100% full: factor = 1.0 (full target_interval sleep)
                    throttle_range = 1.0 - self.throttle_threshold
                    throttle_factor = (fullness - self.throttle_threshold) / throttle_range

                    sleep_time = target_interval * throttle_factor
                    time.sleep(sleep_time)

                    # Track throttling metrics
                    self.debug_stats.record_throttle(sleep_time)

                    if self.debug_stats.enabled and self.debug_stats.packets_sent % 100 == 0:
                        logging.debug(f"[THROTTLE] Queue {current_depth}/{max_size} ({fullness:.0%}), slept {sleep_time*1000:.2f}ms")

    def _sender_thread(self):
        """Dequeue packets and send at precise intervals (dejitter buffer)"""
        # Calculate precise packet interval (256 samples / 48000 Hz)
        packet_interval = self.chunkSize / self.samprate  # 5.333... ms

        if self.debug_stats.enabled:
            logging.info(f"[DEBUG] Packet interval: {packet_interval * 1000:.3f}ms")

        # Wait for buffer to fill before starting (prevents initial stutter)
        queue_size = int(self.config.get('performance', 'queue_size', fallback='20'))
        initial_fill_target = int(self.config.get('performance', 'initial_buffer_fill', fallback='10'))
        initial_fill_target = max(initial_fill_target, 1)  # At least 1 packet

        if self.debug_stats.enabled:
            logging.info(f"[DEBUG] Waiting for buffer to fill to {initial_fill_target} packets...")

        while self.running.is_set() and self.packet_queue.qsize() < initial_fill_target:
            time.sleep(0.1)

        if self.debug_stats.enabled:
            logging.info(f"[DEBUG] Buffer filled to {self.packet_queue.qsize()} packets, starting playback")

        # Use high-precision timer
        next_send_time = time.perf_counter()
        packets_sent_in_window = 0
        timing_corrections = 0

        while self.running.is_set():
            try:
                # Get packet from queue with timeout
                try:
                    chunk, read_time = self.packet_queue.get(timeout=0.1)
                except queue.Empty:
                    if self.debug_stats.enabled:
                        logging.warning("[DEBUG] Buffer underrun - queue empty!")
                    self.debug_stats.record_buffer_underrun()
                    continue

                # Calculate when to send next packet
                next_send_time += packet_interval

                # Sleep until it's time to send
                sleep_time = next_send_time - time.perf_counter()
                if sleep_time > 0:
                    time.sleep(sleep_time)
                elif sleep_time < -0.010:  # More than 10ms behind schedule
                    if self.debug_stats.enabled and packets_sent_in_window % 1000 == 0:
                        logging.warning(f"[DEBUG] Timing drift: {sleep_time * 1000:.2f}ms behind schedule")
                    # Resync to prevent accumulated drift
                    next_send_time = time.perf_counter()
                    timing_corrections += 1
                    self.debug_stats.record_timing_correction()

                self.framecounter += 1
                self.last_active_time = time.time()
                rawData = self._constructFrame(chunk)

                # Measure network send time
                send_start = time.perf_counter()
                try:
                    self.sock.sendto(rawData, (self.toIp, self.toPort))
                except (ConnectionRefusedError, OSError) as e:
                    if self.debug_stats.enabled:
                        logging.warning(f"[DEBUG] Socket error (ignoring): {e}")
                    else:
                        logging.warning(f"Socket error (ignoring): {e}")
                    # UDP is connectionless, these errors are non-fatal
                    self.debug_stats.record_drop()
                send_time = time.perf_counter() - send_start

                # Log if send took unusually long
                if self.debug_stats.enabled and send_time > 0.005:  # > 5ms
                    logging.warning(f"[DEBUG] Slow network send: {send_time*1000:.2f}ms")

                # Record metrics with queue depth
                queue_depth = self.packet_queue.qsize()
                self.debug_stats.record_packet(len(chunk), read_time, send_time, queue_depth)

                packets_sent_in_window += 1

                # Generate periodic report
                if self.debug_stats.should_report():
                    if self.debug_stats.enabled and timing_corrections > 0:
                        logging.info(f"[DEBUG] Timing corrections in last interval: {timing_corrections}")
                        timing_corrections = 0
                    self.debug_stats.generate_report()

                self._update_healthcheck()

            except Exception as e:
                logging.error(f"Error sending packet: {e}")
                # Don't break on individual packet errors, continue processing

    def _queue_resize_monitor(self):
        """Background thread to shrink queue when conditions are stable"""
        CHECK_INTERVAL = 30  # Check every 30 seconds
        SHRINK_THRESHOLD = 0.50  # Shrink if avg < 50% full

        while self.running.is_set():
            time.sleep(CHECK_INTERVAL)

            if not self.enable_queue_growth:
                continue

            current_size = self.packet_queue.maxsize
            if current_size <= self.initial_queue_size:
                continue  # Already at minimum

            # Calculate average queue depth over interval
            if self.debug_stats.queue_depths:
                # Get last 30s of data (assuming ~10Hz sampling)
                recent_depths = list(self.debug_stats.queue_depths)[-300:] if len(self.debug_stats.queue_depths) > 0 else []
                if len(recent_depths) > 0:
                    avg_depth = statistics.mean(recent_depths)
                    avg_fullness = avg_depth / current_size

                    if avg_fullness < SHRINK_THRESHOLD:
                        # Queue is underutilized - shrink it
                        new_size = max(current_size - self.growth_increment, self.initial_queue_size)
                        self._resize_queue(new_size)
                        self.debug_stats.record_queue_shrink()

    def runforever(self, url):
        # Detect stream type for debugging
        self.debug_stats.detect_stream_type(url)

        # Optimized FFmpeg command for low latency
        # Removed -re (rate limiting) and added low-latency flags
        cmd = ["ffmpeg", "-hide_banner",
               "-analyzeduration", "0",
               "-probesize", "32768",
               "-fflags", "+nobuffer+flush_packets",
               "-flags", "low_delay",
               "-i", url,
               "-f", "s16le", "-acodec", "pcm_s16le",
               "-ar", "48000", "-ac", "2",
               "-loglevel", "warning", "-"]

        # Use pipe buffer size from config
        pipe_buffer_size = int(self.config.get('performance', 'pipe_buffer_size', fallback='262144'))

        if self.debug_stats.enabled:
            logging.info(f"[DEBUG] FFmpeg pipe buffer: {pipe_buffer_size} bytes ({pipe_buffer_size/1024:.1f}KB)")

        process = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            bufsize=pipe_buffer_size)

        self.process = process  # Store for cleanup

        # Start FFmpeg reader thread
        ffmpeg_thread = threading.Thread(
            target=self._ffmpeg_thread, args=(process,))
        ffmpeg_thread.daemon = True

        # Start packet sender thread
        sender_thread = threading.Thread(
            target=self._sender_thread)
        sender_thread.daemon = True

        # Start FFmpeg error logging thread
        error_log_thread = threading.Thread(
            target=self._log_errors, args=(process,))
        error_log_thread.daemon = True

        # Start queue resize monitor thread
        resize_monitor_thread = threading.Thread(
            target=self._queue_resize_monitor)
        resize_monitor_thread.daemon = True

        ffmpeg_thread.start()
        sender_thread.start()
        error_log_thread.start()
        resize_monitor_thread.start()

        # Wait for threads to complete or shutdown signal
        ffmpeg_thread.join()
        sender_thread.join(timeout=1)

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

    # Configure logging
    debug_enabled = config.getboolean('debug', 'debug_audio', fallback=False)
    log_level = logging.INFO if debug_enabled else logging.WARNING
    logging.basicConfig(
        level=log_level,
        format='%(levelname)s:%(name)s:%(message)s',
        force=True  # Override any existing config
    )

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
            sender = VBAN_Send(vban_ip, vban_port, vban_name, 48000, config)

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
