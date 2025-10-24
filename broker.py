"""

"""
import socket
import threading
import sys
import time

DEBUG:bool = "--debug" in sys.argv

class Broker:
    def __init__(self, options = ""):
        try:
            self.host = "0.0.0.0"
            self.port = 12345
            self.backlog = 10
            if options:
                extracted_options = self.__extract_data(options)
                self.host = extracted_options.get("h", self.host)
                if "p" in extracted_options:
                    self.port = int(extracted_options["p"])
                if "b" in extracted_options:
                    self.backlog = int(extracted_options["b"])
           
            self.is_running = False
            self.broker_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM) #open the socket
            self.broker_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # enable option: re-using the socket immediately without TIME_WAIT
            self.clients = dict()
            self.subs = dict() #{"topic1":["c1, c2, c3"], "topic2":[c1, c5]}
            self.lock = threading.Lock() # for shared resources
        except Exception as e:
            print(f"Exception: {e}")
            
    def start(self):
        """Start the server and accept clients"""
        self.broker_socket.bind((self.host, self.port))
        self.broker_socket.listen(self.backlog)
        self.is_running = True
        print(f"[BROKER] Listening on {self.host}:{self.port}")
        self.accept_clients()
    
    def accept_clients(self):
        """Accept incoming client connections"""
        while self.is_running:
            try:
                client_socket, client_addr = self.broker_socket.accept()
            except OSError:
                if DEBUG:
                    print("Error While Accepting ...")
                break
            print(f"Connected to {client_addr}.")
            with self.lock:
                self.clients[client_addr] = client_socket # this list yet to be appended with the client's subs
            threading.Thread(target=self.handle_client, daemon=True, args=(client_addr, client_socket)).start() #start the client thread
    
    def handle_client(self, addr, c_socket:socket.socket):
        time.sleep(1)
        try:
            c_socket.sendall(f"[BROKER]connected: {addr}\n".encode())
            if DEBUG:
                print("send Welcome...")
        except Exception as e:
            print(f"[BROKER] Exception wile init send: {e}")
        
        while True:
            # once the connection starts the client must send thier commands
            # sub/pub -t topic [if pub] -m payload
            try:
                data = c_socket.recv(1024)
                if not data:
                    print(f"Disconnected {addr} as no data")
                    break
                msg = data.decode().strip()
                tags_values:dict = self.__extract_data(msg) # returns a dic of: a ==> action, t ==> topic, m ==> payload
                action = tags_values.get("a")
                topic = tags_values.get("t")
                payload = tags_values.get("m", "")
                if "sub" == action:
                    if DEBUG:
                        print(f"Sub received from {addr}")
                    if topic not in self.subs.keys():
                        self.subs[topic] = [c_socket, ]
                    elif c_socket not in self.subs[topic]:
                        self.subs[topic].append(c_socket)
                elif "pub" == action and topic:
                    if DEBUG:
                        print(f"Pub received from {addr}")
                    for c in list(self.subs.get(topic, [])):
                        c.sendall(f"[SEND FROM {addr}]: {topic}: {payload}.".encode())
            except Exception as e:
                print(f"Exception: {e}")
        self.remove_client(addr)
        
    def remove_client(self, addr):
        """Remove a disconnected client."""
        with self.lock:
            if addr in self.clients.keys():
                print(f"[DISCONNECTED] {addr}.")
                self.clients[addr].close()
                del self.clients[addr]
                
                
    def stop(self):
        self.is_running = False
        with self.lock:
            for c in self.clients.values():
                c.close()
        self.clients.clear()
        self.broker_socket.close()
        print("Broker Closed!")
                
    
    def __extract_data(self, data:str)->dict:
        tags_values:dict = {}
        if data.startswith("sub") or data.startswith("pub"): tags_values["a"] = data[:3].strip()
        tags_index = [i for i, l in enumerate(data) if l == "-"]
        for i in range(len(tags_index) - 1):
            tags_values[data[tags_index[i]+1]] = data[tags_index[i]+3:tags_index[i+1]].strip()
        tags_values[data[tags_index[len(tags_index) - 1] + 1]] = data[tags_index[len(tags_index) - 1]+3:].strip()
        return tags_values
    
    
    def __del__(self):
        self.stop()

if __name__ == "__main__":
    options = ""
    if len(sys.argv) > 1:
        options = " ".join(sys.argv[1:])
    broker = Broker(options=options)
    broker.start()