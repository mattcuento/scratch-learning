import os

WAL_FILE = "wal.log"
SNAPSHOT_FILE = "snapshot.db"
BUFFER_SIZE = 4096  # Read buffer size for WAL replay

class WriteAheadLog:
    def __init__(self, wal_file=WAL_FILE):
        self.wal_file = wal_file
        self._ensure_wal_exists()

    def _ensure_wal_exists(self):
        """Create the WAL file if it doesn't exist."""
        if not os.path.exists(self.wal_file):
            fd = os.open(self.wal_file, os.O_CREAT | os.O_WRONLY)
            os.close(fd)

    def log_operation(self, operation, key, value=None):
        """Append an operation (put/delete) to the WAL."""
        fd = os.open(self.wal_file, os.O_APPEND | os.O_WRONLY)
        try:
            entry = f"{operation} {key} {value if value else ''}\n"
            os.write(fd, entry.encode())  # Convert string to bytes
            os.fsync(fd)  # Ensure the write is persisted
        finally:
            os.close(fd)

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
        finally:
            os.close(fd)
        os.rename(SNAPSHOT_FILE + ".tmp", SNAPSHOT_FILE)  # Atomic replace
        fd = os.open(WAL_FILE, os.O_WRONLY | os.O_TRUNC)
        os.close(fd)

# Example usage
if __name__ == "__main__":
    kv = KeyValueStore()
    kv.put("name", "Matt")
    kv.put("lang", "Python")
    kv.delete("lang")
    kv.checkpoint()  # Save snapshot and truncate WAL