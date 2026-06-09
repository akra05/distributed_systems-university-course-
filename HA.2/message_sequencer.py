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
    task_id: int

#worker thread 
#receives two types of messsages
#external message: message from simulated client, written into the worker queue and propagated to the sequencer
#internal message: message from sequencer, stored into the worker history

def worker(thread_id, queue, sequencer_queue, histories):
    history = []
    # run until all tasks are propagated
    while True:
        msg = queue.get()
        #if true sequencer send an stop signal. None in queue == Stop signal from sequencer
        if msg is None:
            print(f"Thread {thread_id} is finished")

            #write histories in file . path e.g. logs/thread_id_1.log
            with open(f"logs/thread_id_{thread_id}.log","w") as f:
                for i in history:
                    f.write(str(i)+"\n")

            histories.append(history)
            break
        #if message is external, message is propagated to the seqeuencer
        if isinstance(msg,ExternalMessage):
            internal_message = InternalMessage(msg.payload,thread_id,-1)
            sequencer_queue.put(internal_message)
        #if message is internal, thread writes answer from the sequencer into the history list
        if isinstance(msg,InternalMessage):
            history.append(msg)
            print(msg)

    if thread_id == 1:
        print(history) 

#sequencer: receives messages only from workers, defines an order and broadcasts the message order with an individual id per message
def sequencer(dict):
    id = 0
    while True:
        msg = dict["sequencer"].get()

        if msg is None:
            break

        answer_message = InternalMessage(msg.payload,msg.thread_id,id)

        for i in range(NUMBER_OF_THREADS):
            dict[i].put(answer_message)
        id = id + 1

#simulated client: sends messages in random time intervals to a random worker with message_type: external
def client(dict):
    print("I am the client")
    for i in range(NUMBER_OF_MESSAGES):
        time.sleep(random.uniform(0.1,1))
        q = dict[random.randint(0,NUMBER_OF_THREADS-1)]
        q.put(ExternalMessage(i))
    print("Now I am finished")

    
#creates a queue for each thread
def create_queues():
    q = {}

    for i in range(NUMBER_OF_THREADS):
        q[i] = queue.Queue()

    q["sequencer"] = queue.Queue()

    return q

def main():

    #create logs folder
    os.makedirs("logs", exist_ok=True)

    q = create_queues()

    #create threads 1. sequencer thread and NUMBER_OF_THREADS workerthreads   
    threads = []
    histories = []

    t = threading.Thread(target=sequencer,args=(q,))
    threads.append(t)
    for i in range(NUMBER_OF_THREADS):
        
        t = threading.Thread(target=worker,args=(i,q[i],q["sequencer"],histories))
        threads.append(t)

    t = threading.Thread(target=client, args=(q,))
    threads.append(t)
    # Start each thread
    for t in threads[:-1]:
        t.start()

    #start client after all worker threads are started
    threads[-1].start()
    threads[-1].join()

    q["sequencer"].put(None)

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