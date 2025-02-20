import threading
import time
from threading import Thread

def thread_task():
    print(f"{threading.current_thread()}")
    time.sleep(5)

def main():
    t1 = Thread(target=thread_task)
    t1.start()

    t1.join(2)

    if t1.is_alive():
        print("thread timed out")
    else:
        print("thread alive")

def join_main_thread_exc():
    threading.main_thread().join()

def simple_mutex():

    l1 = threading.Lock()

    def get_lock_and_raise(lock: threading.Lock):
        lock.acquire(timeout=5)
        raise Exception("Oh no!")

    t1 = Thread(target=get_lock_and_raise, args=(l1,))
    t2 = Thread(target=get_lock_and_raise, args=(l1,))

    t1.start()
    t2.start()

    t1.join(timeout=2)
    t2.join(timeout=2)

    if t1.is_alive():
        print("Couldn't get the lock in t1, timed out")
    if t2.is_alive():
        print("Couldn't get the lock in t2, timed out")


def house_of_cards():
    bar = threading.Barrier(parties=5, action=lambda: print("Break 'em down"))

    def add_a_card():
        print(f"Added card")
        i = bar.wait()

    threads = [Thread(target=add_a_card) for _ in range(5)]

    for t in threads:
        t.start()

    for t in threads:
        t.join()

if __name__ == "__main__":
    house_of_cards()
