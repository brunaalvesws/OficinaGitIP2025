import zmq
import pickle
import random
import time
import threading

NWORKERS = 3  # número de workers

#Início do código

def producer():
    context = zmq.Context()
    socket = context.socket(zmq.PUSH)  # create a push socket
    socket.bind("tcp://127.0.0.1:12345")  # bind socket to address

    while True:
        workload = random.randint(1, 5)  # workload menor para testes rápidos
        print(f"Produzindo workload: {workload}")
        socket.send(pickle.dumps(workload))  # send workload to worker
        time.sleep(workload / NWORKERS)  # balance production by waiting

def worker(id):
    context = zmq.Context()
    socket = context.socket(zmq.PULL)  # create a pull socket
    socket.connect("tcp://127.0.0.1:12345")  # connect to the producer

    while True:
        work = pickle.loads(socket.recv())  # receive work from producer
        print(f"Worker {id} processando workload: {work}")
        time.sleep(work)  # simulate work

if __name__ == "__main__":
    # criar thread do producer
    threading.Thread(target=producer, daemon=True).start()

    # criar threads dos workers
    for i in range(NWORKERS):
        threading.Thread(target=worker, args=(i,), daemon=True).start()

    # manter o programa rodando
    while True:
        time.sleep(1)
