"""

"""
import socket
import threading
import sys

class Broker:
    def __init__(self, options = ""):
        try:
            if not options:
                self.host = "0.0.0.0"
                self.port = 12345
                self.backlog = 10
            else:
                extracted_options = self.extract_data(options)
                self.host = extracted_options.get("h")
                self.port = extracted_options.get("p")
                self.backlog = extracted_options.get("b")
            self.is_running = False
            self.broker_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM) #open the socket
            self.broker_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # enable option: re-using the socket immediately without TIME_WAIT
            self.clients = dict()
            self.subs = dict() #{"topic1":["c1, c2, c3"], "topic2":[c1, c5]}
            self.lock = threading.local() # for shared resources
            self.start()
        except Exception as e:
            print(f"Exception: {e}")
            
    def start(self):
        """Start the server and accept clients"""
        self.broker_socket.bind((self.host, self.port))
        self.broker_socket.listen(self.backlog)
        self.is_running = True
        print(f"[BROKER] Listening on {self.host}:{self.port}")
        threading.Thread(target=self.accept_clients, daemon=True).start()
    
    def accept_clients(self):
        """Accept incoming client connections"""
        while self.is_running:
            try:
                client_addr, client_socket = self.broker_socket.accept()
                with self.lock:
                    self.clients[client_addr] = [client_socket] # this list yet to be appended with the client's subs
                threading.Thread(target=self.handle_client, daemon=True, args=(client_addr, client_socket)).start() #start the client thread
            except Exception as e:
                print(f"Exception: {e}")
    
    def handle_client(self, addr, c_socket):
        c_socket.sendall(f"connceted: {addr}".encode())
        while True:
            # once the connection starts the client must send thier commands
            # sub/pub -t topic [if pub] -m payload
            data = c_socket.recv(1024).decode().strip()
            if not data:
                    print(f"Disconnected {addr} as no data")
                    break
            with self.lock:
                try:
                    tags_values:dict = self.extract_data(data) # returns a dic of: a ==> action, t ==> topic, m ==> payload
                    action = tags_values.get("a")
                    topic = tags_values.get("t")
                    payload = tags_values.get("m")
                    if "sub" == action:
                        self.subs[topic].append(c_socket)
                    elif "pub" == action and topic in self.subs.keys():
                        for c in self.subs[topic]:
                            c.sendall(f"[SEND FROM {addr}]: {payload}.".encode())
                except Exception as e:
                    print(f"Exception: {e}")
        #@TODO: add clients termination actions: kill thier task, remove thier subs.
        
    def stop(self):
        self.is_running = False
        with self.lock:
            for c in self.clients.values():
                c.close()
        self.clients.clear()
        self.broker_socket.close()
        print("Broker Closed!")
                
    
    def extract_data(self, data:str)->dict:
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