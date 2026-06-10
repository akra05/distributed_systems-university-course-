import threading
import queue
import time
import random 
import sys
import os
from dataclasses import dataclass

NUMBER_OF_THREADS =  int(sys.argv[1])
NUMBER_OF_MESSAGES = int(sys.argv[2])

@dataclass(frozen=True)
class ExternalMessage:
    payload: int

@dataclass(frozen=True)
class InternalMessage:
    payload: int
    thread_id: int
    lamport_clock: int

    def __lt__(self, other):
        if self.lamport_clock < other.lamport_clock:
            return True
        if self.lamport_clock > other.lamport_clock:
            return False
        
        if self.thread_id < other.thread_id:
            return True
        else:
            return False

def worker(thread_id,dict,histories):
    lamport_clock = 0
    history = []
    while True:
        msg = dict[thread_id].get()

        if msg is None:
            history.sort()
            histories.append(history)

            with open(f"logs/thread_id_{thread_id}.log","w") as f:
                for i in history:
                    f.write(str(i)+"\n")
            break
        
        if isinstance(msg, ExternalMessage):
            internal_message = InternalMessage(msg.payload,thread_id,lamport_clock+1)
            for i in dict:
                dict[i].put(internal_message)
            lamport_clock = lamport_clock + 1 
        
        if isinstance(msg, InternalMessage):
            lamport_clock = max(lamport_clock, msg.lamport_clock) + 1
            history.append(msg)
            print(msg)

def client(dict):
    print("I am the client")
    for i in range(NUMBER_OF_MESSAGES):
        time.sleep(random.uniform(0.1,1))
        q = dict[random.randint(0,NUMBER_OF_THREADS-1)]
        q.put(ExternalMessage(i))
    print("Now I am finished")

def create_queues():
    q = {}

    for i in range(NUMBER_OF_THREADS):
        q[i] = queue.Queue()

    return q



def main():

    #create logs folder
    os.makedirs("logs", exist_ok=True)

    q = create_queues()

    #create threads 1. sequencer thread and NUMBER_OF_THREADS workerthreads   
    threads = []
    histories = []

    for i in range(NUMBER_OF_THREADS):
        
        t = threading.Thread(target=worker,args=(i,q,histories))
        threads.append(t)

    t = threading.Thread(target=client, args=(q,))
    threads.append(t)
    # Start each thread
    for t in threads[:-1]:
        t.start()

    #start client after all worker threads are started
    threads[-1].start()
    threads[-1].join()

    while any(not q[i].empty() for i in range(NUMBER_OF_THREADS)):
        time.sleep(0.1)

    for i in range(NUMBER_OF_THREADS):
        q[i].put(None)
    
    # Wait for all threads to finish
    for t in threads:
        t.join()
    
    #check if all queues have the same task order
    is_equal = True
    for i in histories:
        if i != histories[0]:
            print("worker lists are not equal")
            is_equal = False
            break
    
    if is_equal:
        print("all worker lists are identical: Program works")
            

if __name__ == "__main__":
    main()