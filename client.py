"""

"""
import socket
import threading
import sys
import time

DEBUG:bool = "--debug" in sys.argv

class Client:
    def __init__(self, options:str = ""):
        try:
            self.host = "localhost"
            self.port = 12345

            if options:
                opts = self.__extract_data(options)
                self.host = opts.get("h", self.host)
                if "p" in opts:
                    self.port = int(opts["p"])
                    
            self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.connected = False
            self.started = False
            self.lock = threading.Lock()
        except Exception as e:
            print(f"Exception: {e}")
            
    def connect(self):
        while True:
            try:
                self.client_socket.connect((self.host, self.port))
                self.connected = True
                break
            except Exception as e:
                print(f"Exception: {e}\n reconnecting after 3 seconds...")
                time.sleep(2)
                
    def listen_to_server(self):
        while self.started:
            try:
                if not self.connected:
                    self.connect()
                
                data = self.client_socket.recv(1024)
                if not data:
                    if DEBUG:
                        print("Recieved No Data...")
                    continue
                print(data.decode().strip(), ">> ", sep="\n", end=" ", flush=True)
            except Exception as e:
                print(f"Exception: {e}")
                self._reconnect()
    
    def send_to_server(self):
        while self.started:
            try:
                if not self.connected:
                    self.connect()
                msg = input(">> ")
                if DEBUG:
                    print("Input Recived....")
                with self.lock:
                    self.client_socket.sendall(msg.encode())
                if DEBUG:
                    print("Input Send ....")
            except Exception as e:
                print(f"Exception: {e}")
                self._reconnect()
                
    def _reconnect(self):
        # with self.lock:
        #     try:
        #         self.client_socket.close()
        #     except Exception:
        #         pass
        #     self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #     self.connected = False
        time.sleep(0.1)
                
    def start(self):
        self.started = True
        threading.Thread(target=self.listen_to_server, daemon = True).start()
        time.sleep(1)
        threading.Thread(target=self.send_to_server, daemon=True).start()
        while self.started:
            time.sleep(0.2)
    
    def close(self):
        self.started = False
        self.client_socket.close()
        self.connected = False
        
    def __del__(self):
        self.close()
    
    def __extract_data(self, data:str)->dict:
        tags_values:dict = {}
        if data.startswith("sub") or data.startswith("pub"): tags_values["a"] = data[:3].strip()
        tags_index = [i for i, l in enumerate(data) if l == "-"]
        for i in range(len(tags_index) - 1):
            tags_values[data[tags_index[i]+1]] = data[tags_index[i]+3:tags_index[i+1]].strip()
        tags_values[data[tags_index[len(tags_index) - 1] + 1]] = data[tags_index[len(tags_index) - 1]+3:].strip()
        return tags_values
    

if __name__ == "__main__":
    options = ""
    if len(sys.argv) > 1:
        options = " ".join(sys.argv[1:])
    client = Client(options=options)
    client.start()