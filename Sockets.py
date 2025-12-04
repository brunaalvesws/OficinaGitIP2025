import socket
import pickle
import threading

# ==== CONSTANTES ====
CREATE = "CREATE"
APPEND = "APPEND"
GETVALUE = "GETVALUE"
OK = "OK"

HOST = "localhost"
PORT_SERVER = 3000
PORTC1 = 5001
PORTC2 = 5002


# ==== CLASSE DBClient ====
class DBClient:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.listID = None

    def __sendrecv(self, message):
        sock = socket.socket()
        sock.connect((self.host, self.port))
        sock.send(pickle.dumps(message))
        result = pickle.loads(sock.recv(4096))
        sock.close()
        return result

    def create(self):
        self.listID = self.__sendrecv([CREATE])
        return self.listID

    def getValue(self):
        return self.__sendrecv([GETVALUE, self.listID])

    def appendData(self, data):
        return self.__sendrecv([APPEND, data, self.listID])


# ==== CLASSE SERVER ====
class Server:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.setOfLists = {}
        self.sock = socket.socket()
        self.sock.bind((self.host, self.port))
        self.sock.listen(5)
        print(f"[SERVER] Aguardando conexões em {self.host}:{self.port}...")

    def run(self):
        while True:
            conn, addr = self.sock.accept()
            data = conn.recv(4096)
            request = pickle.loads(data)

            if request[0] == CREATE:
                listID = len(self.setOfLists) + 1
                self.setOfLists[listID] = []
                conn.send(pickle.dumps(listID))

            elif request[0] == APPEND:
                listID = request[2]
                data_item = request[1]
                self.setOfLists[listID].append(data_item)
                conn.send(pickle.dumps(OK))

            elif request[0] == GETVALUE:
                listID = request[1]
                result = self.setOfLists[listID]
                conn.send(pickle.dumps(result))

            conn.close()


# ==== CLASSE CLIENT ====
class Client:
    def __init__(self, port):
        self.host = "localhost"
        self.port = port
        self.sock = socket.socket()
        self.sock.bind((self.host, self.port))
        self.sock.listen(2)

    def sendTo(self, host, port, data):
        sock = socket.socket()
        sock.connect((host, port))
        sock.send(data)  # já é pickle.dumps() do lado do cliente
        sock.close()

    def recvAny(self):
        conn, addr = self.sock.accept()
        data = b""
        while True:
            chunk = conn.recv(4096)
            if not chunk:
                break
            data += chunk
        conn.close()
        return data


# ==== FUNÇÕES DOS CLIENTES ====
def client1():
    c1 = Client(PORTC1)
    dbC1 = DBClient(HOST, PORT_SERVER)
    dbC1.create()
    dbC1.appendData("Client 1")
    print("[Client1] Enviando referência ao Client2...")
    c1.sendTo(HOST, PORTC2, pickle.dumps(dbC1))


def client2():
    c2 = Client(PORTC2)
    print("[Client2] Aguardando referência do Client1...")
    data = c2.recvAny()
    dbC2 = pickle.loads(data)  # <- aqui está certo
    dbC2.appendData("Client 2")
    print("[Client2] Lista atual:", dbC2.getValue())


# ==== EXECUÇÃO ====
if __name__ == "__main__":
    server = Server(HOST, PORT_SERVER)
    threading.Thread(target=server.run, daemon=True).start()

    threading.Thread(target=client1).start()
    threading.Thread(target=client2).start()
