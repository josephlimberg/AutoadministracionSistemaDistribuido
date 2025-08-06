import socket
import pickle
import threading
import time
import psutil
import random

class WorkerNode:
    def __init__(self, master_host, master_port=6000, report_interval=5):
        self.master_host = master_host
        self.master_port = master_port
        self.report_interval = report_interval
        self.running = True
        self.load = 1.0  # Carga inicial

        # Hilos para métricas, comandos y carga
        self.metrics_thread = threading.Thread(target=self.report_metrics)
        self.command_thread = threading.Thread(target=self.listen_commands)
        self.work_thread = threading.Thread(target=self.simulate_work)

        self.metrics_thread.start()
        self.command_thread.start()
        self.work_thread.start()

    def get_metrics(self):
        return {
            'cpu': psutil.cpu_percent(),
            'mem': psutil.virtual_memory().percent,
            'load': self.load
        }

    def report_metrics(self):
        while self.running:
            metrics = self.get_metrics()
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((self.master_host, self.master_port))
                    s.send(pickle.dumps(metrics))
                print(f"📤 Métricas enviadas al Maestro ({self.master_host}:{self.master_port})")
            except Exception as e:
                print(f"❌ Error enviando métricas: {e}")
            time.sleep(self.report_interval)

    def listen_commands(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('0.0.0.0', 6001))  # Puerto para comandos
            s.listen()
            while self.running:
                conn, addr = s.accept()
                data = conn.recv(1024)
                if data:
                    command = pickle.loads(data)
                    if command["action"] == "reduce_load":
                        self.load *= command["value"]
                        print(f"🔽 Carga reducida a {self.load:.2f}")
                conn.close()

    def simulate_work(self):
        while self.running:
            # Simular carga variable (aumenta si está por debajo del 90%)
            if self.load < 90:
                self.load += random.uniform(5, 20)
            else:
                self.load = random.uniform(1, 10)  # Resetear carga
            time.sleep(2)

if __name__ == "__main__":
    worker = WorkerNode(
        master_host="192.168.100.87",  # IP local del Master
        master_port=6000
    )
    print("\n🔹 Worker Iniciado")
    print(f"🔗 Conectando al Maestro en: {worker.master_host}:{worker.master_port}")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        worker.running = False
        print("\n🛑 Worker detenido manualmente")