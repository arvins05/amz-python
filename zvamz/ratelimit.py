import time
import threading
import datetime
import requests

class RateLimiter:
    def __init__(self, tokens_per_second, capacity):
        self.capacity = capacity
        self.tokens = capacity
        if tokens_per_second <= 0:
            raise Exception("tokens_per_second must be greater than 0")
        self.tokens_per_second = tokens_per_second
        self.lock = threading.Lock()
        self.last_refill_time = time.time()

    def refill(self):
        """Refill tokens according to elapsed time."""
        now = time.time()
        elapsed = now - self.last_refill_time

        tokens_to_add = elapsed * self.tokens_per_second
        if tokens_to_add > 0:
            self.tokens = min(self.capacity, self.tokens + tokens_to_add)
            self.last_refill_time = now

    def allow_request(self):
        """Allow or deny a request based on available tokens."""
        with self.lock:
            self.refill()
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            else:
                return False

    def send_request(self, action, *args, **kwargs):
        """Send a request, handling throttling."""
        while not self.allow_request():
            time.sleep(1)

        current_timestamp = time.time()
        current_datetime = datetime.datetime.fromtimestamp(current_timestamp).isoformat()
        print(f"Request allowed: {current_datetime}")

        try:
            response = action(*args, **kwargs)

            # Handle throttling based on the response status code
            if response.status_code == 429:  # Too Many Requests
                print("Throttled: too many requests. Retrying...")
                self.tokens = 0
                time.sleep(1 / self.tokens_per_second)  # Adjust the delay based on rate limits
                return self.send_request(action, *args, **kwargs)

        except requests.exceptions.RequestException as e:
            # Handle other request exceptions, like network errors, etc.
            print(f"Request failed: {e}")
            return None

        return response