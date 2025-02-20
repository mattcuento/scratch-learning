import os
import threading
import time

WAL_FILE = "wal.log"
SNAPSHOT_FILE = "snapshot.db"
BUFFER_SIZE = 4096  # Read buffer size for WAL replay


class WriteAheadLog:
    def __init__(self, wal_file=WAL_FILE):
        self.wal_file = wal_file
        self._ensure_wal_exists()
        self._wal_fd = os.open(self.wal_file, os.O_APPEND | os.O_WRONLY)
        self._wal_wl = threading.Lock()

    def _ensure_wal_exists(self):
        """Create the WAL file if it doesn't exist."""
        if not os.path.exists(self.wal_file):
            fd = os.open(self.wal_file, os.O_CREAT | os.O_WRONLY)
            os.close(fd)

    def log_operation(self, operation, key, value=None):
        """Append an operation (put/delete) to the WAL."""
        with self._wal_wl:
            entry = f"{operation} {key} {value if value else ''}\n"
            print(f"Writing entry to WAL: {entry}")
            os.write(self._wal_fd, entry.encode())  # Convert string to bytes
            os.fsync(self._wal_fd)  # Ensure the write is persisted

    def close_wal(self):
        with self._wal_wl:
            os.close(self._wal_fd)

    def replay(self):
        """Replay WAL to restore state."""
        store = {}
        if os.path.exists(self.wal_file):
            fd = os.open(self.wal_file, os.O_RDONLY)
            try:
                data = b""
                while True:
                    chunk = os.read(fd, BUFFER_SIZE)
                    if not chunk:
                        break
                    data += chunk
                lines = data.decode().splitlines()
                for line in lines:
                    parts = line.split()
                    if len(parts) < 2:
                        continue
                    operation, key = parts[0], parts[1]
                    value = parts[2] if len(parts) > 2 else None
                    if operation == "put":
                        store[key] = value
                    elif operation == "delete" and key in store:
                        del store[key]
            finally:
                os.close(fd)
        return store

    def truncate(self):
        with self._wal_wl:
            os.ftruncate(self._wal_fd, 0)

    def stream_wal(self):
        """Stream WAL entries in real-time using os.lseek()."""
        fd = os.open(self.wal_file, os.O_RDONLY)
        try:
            file_pos = os.lseek(fd, 0, os.SEEK_END)  # Start at EOF to avoid reading old logs
            while True:
                # Seek to the current position and read new data
                os.lseek(fd, file_pos, os.SEEK_SET)
                chunk = os.read(fd, BUFFER_SIZE)
                if chunk:
                    file_pos = os.lseek(fd, 0, os.SEEK_CUR)  # Update file pointer to last read position
                    print("New Data:")
                    print(chunk.decode(), end="")  # Process the new data
                else:
                    # No new data, continue checking
                    os.lseek(fd, file_pos, os.SEEK_SET)  # Ensure seeking stays at the current position
                    continue
        finally:
            os.close(fd)


class KeyValueStore:
    def __init__(self):
        self.store = {}
        self.wal = WriteAheadLog()
        self.load_from_snapshot()
        self.recover_from_wal()

    def put(self, key, value):
        """Write key-value pair with WAL logging."""
        self.wal.log_operation("put", key, value)
        self.store[key] = value

    def get(self, key):
        """Retrieve value by key."""
        return self.store.get(key)

    def delete(self, key):
        """Delete key with WAL logging."""
        if key in self.store:
            self.wal.log_operation("delete", key)
            del self.store[key]

    def load_from_snapshot(self):
        """Load state from a snapshot file."""
        if os.path.exists(SNAPSHOT_FILE):
            fd = os.open(SNAPSHOT_FILE, os.O_RDONLY)
            try:
                data = os.read(fd, BUFFER_SIZE).decode()
                for line in data.splitlines():
                    key, value = line.split(" ", 1)
                    self.store[key] = value
            finally:
                os.close(fd)

    def recover_from_wal(self):
        """Recover state from WAL."""
        self.store.update(self.wal.replay())

    def checkpoint(self):
        """Save current state to snapshot and truncate WAL."""
        fd = os.open(SNAPSHOT_FILE + ".tmp", os.O_CREAT | os.O_WRONLY | os.O_TRUNC)
        try:
            for key, value in self.store.items():
                os.write(fd, f"{key} {value}\n".encode())
            os.fsync(fd)
        finally:
            os.close(fd)
        os.rename(SNAPSHOT_FILE + ".tmp", SNAPSHOT_FILE)  # Atomic replace
        fd = os.open(WAL_FILE, os.O_WRONLY | os.O_TRUNC)
        os.close(fd)


# Writer thread function
def writer_thread(kv_store):
    """Simulates periodic WAL writes."""
    for i in range(10):
        key = f"key{i}"
        value = f"value{i}"
        kv_store.put(key, value)
        print(f"Writer: Added {key} = {value}")
        time.sleep(1)  # Simulate delay between writes


# Reader thread function
def reader_thread(wal):
    """Simulates real-time WAL streaming."""
    wal.stream_wal()


def main_stream_wal():
    kv = KeyValueStore()

    # Start writer and reader threads
    writer = threading.Thread(target=writer_thread, args=(kv,))
    reader = threading.Thread(target=reader_thread, args=(kv.wal,))

    writer.start()
    reader.start()

    # Join threads to wait for completion
    writer.join()
    reader.join()


def main_simple_restore():
    kv = KeyValueStore()
    kv.put("name", "Matt")
    kv.put("lang", "Python")
    kv.put("key", "TEST")
    print(kv.wal.replay())
    kv.delete("lang")
    kv.checkpoint()  # Save snapshot and truncate WAL
    print(kv.wal.replay())

    kv.delete("name")
    kv.delete("key")

    print(kv.wal.replay())

    print(kv.store)

    print("Crashing!")

    kv.store = {}

    kv.load_from_snapshot()

    print("Recovered!")

    print(kv.store)


# Example usage
if __name__ == "__main__":
    main_stream_wal()
