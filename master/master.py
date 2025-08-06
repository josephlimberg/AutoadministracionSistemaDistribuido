# PC1 - MasterNode (maestro)

import socket
import threading
import pickle
import time
from psutil import cpu_percent, virtual_memory
import mysql.connector

class MasterNode:
    def __init__(self, host='0.0.0.0', port=6000):
        self.host = host
        self.port = port
        self.workers = {}  # {ip: {cpu, mem, load, timestamp}}
        self.tasks = {}    # {task_id: {"status": "pending/assigned", "worker": ip}}
        self.running = True

        self.db_config = {
            'host': 'localhost',
            'port' : '3306',
            'user': 'joseph',
            'password': 'joseph',
            'database': 'lab_distribuidos'
        }

        self.server_thread = threading.Thread(target=self.run_server)
        self.server_thread.start()

        self.monitor_thread = threading.Thread(target=self.monitor_workers)
        self.monitor_thread.start()

        self.task_thread = threading.Thread(target=self.simulate_tasks)
        self.task_thread.start()

    def run_server(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, self.port))
            s.listen()
            print(f"🔷 Maestro escuchando en {self.host}:{self.port}")
            while self.running:
                conn, addr = s.accept()
                data = conn.recv(4096)
                if data:
                    worker_ip = addr[0]
                    metrics = pickle.loads(data)
                    metrics["timestamp"] = time.time()
                    self.workers[worker_ip] = metrics
                    print(f"📊 Métricas de {worker_ip}: CPU={metrics['cpu']}%, MEM={metrics['mem']}%")
                    self.save_metrics_to_db(worker_ip, metrics)
                conn.close()

    def save_metrics_to_db(self, ip, metrics):
        try:
            conn = mysql.connector.connect(**self.db_config)
            cursor = conn.cursor()
            query = "INSERT INTO metrics (ip, cpu, mem, timestamp) VALUES (%s, %s, %s, NOW())"
            values = (ip, metrics["cpu"], metrics["mem"])
            cursor.execute(query, values)
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as e:
            print(f"❌ Error guardando en DB: {e}")

    def monitor_workers(self):
        while self.running:
            time.sleep(5)
            if not self.workers:
                print("⚠ No hay workers conectados")
                continue

            current_time = time.time()
            inactive = [ip for ip, m in self.workers.items() if current_time - m["timestamp"] > 10]
            for ip in inactive:
                del self.workers[ip]
                print(f"❌ Worker {ip} inactivo")
                self.reassign_tasks(ip)

            for ip, metrics in self.workers.items():
                if metrics['cpu'] > 80:
                    print(f"⚠ Worker {ip} sobrecargado. Enviando comando...")
                    self.send_command(ip, "reduce_load", 0.5)

            self.assign_pending_tasks()

    def send_command(self, ip, action, value):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((ip, 6001))
                s.send(pickle.dumps({"action": action, "value": value}))
        except Exception as e:
            print(f"❌ Error enviando comando a {ip}: {e}")

    def simulate_tasks(self):
        task_id = 0
        while self.running:
            self.tasks[f"task_{task_id}"] = {"status": "pending", "worker": None}
            print(f"📦 Nueva tarea generada: task_{task_id}")
            task_id += 1
            time.sleep(10)

    def assign_pending_tasks(self):
        for tid, info in list(self.tasks.items()):
            if info["status"] == "pending":
                best = min(self.workers.items(), key=lambda x: x[1]['cpu'], default=None)
                if best:
                    self.tasks[tid]["status"] = "assigned"
                    self.tasks[tid]["worker"] = best[0]
                    print(f"✅ Tarea {tid} asignada a {best[0]}")

    def reassign_tasks(self, dead_ip):
        for tid, info in list(self.tasks.items()):
            if info["worker"] == dead_ip:
                self.tasks[tid]["status"] = "pending"
                self.tasks[tid]["worker"] = None
                print(f"🔄 Reasignando tarea {tid}...")

if __name__ == "__main__":
    master = MasterNode()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        master.running = False
        print("\n🛑 Maestro detenido")
        