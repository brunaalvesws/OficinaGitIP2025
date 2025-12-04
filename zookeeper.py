"""
distributed_lock.py

docker start zookeeper

Implementação simples de um lock distribuído usando ZooKeeper (kazoo).
Algoritmo: criar znode efêmero sequencial em /locks/<name>/lock-,
verificar se é o menor filho -> se sim, adquiriu. Senão, espera a deleção
do predecessor (watch) e tenta de novo.
"""

import time
import threading
from kazoo.client import KazooClient
from kazoo.exceptions import KazooException, NodeExistsError
import os

class DistributedLock:
    def __init__(self, zk_hosts: str, lock_root: str = "/locks", lock_name: str = "my_lock", client_id: str = None):
        """
        zk_hosts: host:port (ex: '127.0.0.1:2181')
        lock_root: diretório raiz para locks (será criado se não existir)
        lock_name: nome do lock (cada recurso usa um lock_name distinto)
        client_id: identificador legível do cliente (opcional)
        """
        self.zk_hosts = zk_hosts
        self.lock_root = lock_root.rstrip("/")
        self.lock_name = lock_name
        self.client_id = client_id or f"client-{os.getpid()}-{threading.get_ident()}"
        self.zk = KazooClient(hosts=self.zk_hosts)
        self.node_path = None  # caminho do znode efêmero sequencial criado
        self.acquired = False
        self._cond = threading.Condition()

        self.zk.start()
        # garante que /locks e /locks/<lock_name> existam
        self._ensure_paths()

    def _ensure_paths(self):
        root = self.lock_root
        name_path = f"{root}/{self.lock_name}"
        # use makepath=True para criar recursivamente
        self.zk.ensure_path(root)
        self.zk.ensure_path(name_path)

    def acquire(self, timeout: float = None) -> bool:
        """
        Tenta adquirir o lock. Retorna True se adquirido, False se timeout.
        Timeout em segundos (None -> bloqueia indefinidamente).
        """
        base = f"{self.lock_root}/{self.lock_name}/lock-"
        # cria znode efêmero sequencial
        try:
            self.node_path = self.zk.create(base, value=self.client_id.encode('utf-8'),
                                            ephemeral=True, sequence=True)
        except KazooException as e:
            raise RuntimeError("Erro ao criar znode no ZooKeeper: " + str(e))

        # loop: verificar se somos o menor; se não, esperar predecessor
        start = time.time()
        while True:
            children = self.zk.get_children(f"{self.lock_root}/{self.lock_name}")
            # ordenar pelo sufixo sequencial (o kazoo retorna nomes, ex: lock-0000000003)
            children_sorted = sorted(children)
            # extrai nosso nome de node_path (apenas o último segmento)
            our_name = self.node_path.split("/")[-1]

            # se somos o primeiro -> adquirimos
            if children_sorted and children_sorted[0] == our_name:
                self.acquired = True
                return True

            # caso contrário, encontrar predecessor imediato
            idx = children_sorted.index(our_name)
            predecessor = children_sorted[idx - 1]
            predecessor_path = f"{self.lock_root}/{self.lock_name}/{predecessor}"

            # definimos watch para a exclusão do predecessor
            event_happened = threading.Event()

            @self.zk.DataWatch(predecessor_path)
            def _watch(data, stat, event):
                # DataWatch dispara para mudanças e para remoção (stat None)
                # quando predecessor some, sinalizamos
                if stat is None:
                    event_happened.set()
                    return False  # cancela o watch
                # senão, continuamos observando
                return True

            # calcula tempo restante para timeout, se houver
            if timeout is None:
                waited = event_happened.wait()  # espera até predecessor sumir
            else:
                elapsed = time.time() - start
                remaining = timeout - elapsed
                if remaining <= 0:
                    # timeout estourou: limpamos nosso znode e retornamos False
                    try:
                        self.zk.delete(self.node_path)
                    except Exception:
                        pass
                    self.node_path = None
                    return False
                waited = event_happened.wait(timeout=remaining)

            # se o evento foi sinalizado (predecessor removido), re-loop para verificar
            # se timeout expirou no while, será tratado na próxima iteração
            # continue looping até adquirir ou timeout
            continue

    def release(self):
        """
        Libera o lock (deleta o znode criado). Se já liberado ou nulo, ignora.
        """
        if self.node_path:
            try:
                self.zk.delete(self.node_path)
            except Exception:
                pass
            self.node_path = None
            self.acquired = False

    def close(self):
        """Fecha a conexão com ZooKeeper."""
        try:
            if self.node_path:
                self.release()
            self.zk.stop()
            self.zk.close()
        except Exception:
            pass

# Exemplo de uso: simula vários "clientes" (threads) tentando adquirir o mesmo lock
if __name__ == "__main__":
    import random
    from concurrent.futures import ThreadPoolExecutor

    ZK_HOSTS = "127.0.0.1:2181"
    LOCK_ROOT = "/locks"
    LOCK_NAME = "recursoA"

    def worker(name: str, sleep_hold=2.0):
        lock = DistributedLock(ZK_HOSTS, LOCK_ROOT, LOCK_NAME, client_id=name)
        print(f"[{name}] tentando adquirir lock...")
        got = lock.acquire(timeout=10.0)
        if not got:
            print(f"[{name}] não conseguiu adquirir lock (timeout).")
            lock.close()
            return

        print(f"[{name}] lock adquirido! executando trabalho por {sleep_hold:.1f}s")
        time.sleep(sleep_hold)  # simula trabalho na região crítica
        print(f"[{name}] liberando lock.")
        lock.release()
        lock.close()

    # criar algumas threads simulando clientes distintos
    clients = [f"cliente-{i}" for i in range(5)]
    with ThreadPoolExecutor(max_workers=5) as ex:
        futures = []
        for c in clients:
            t = random.uniform(1.0, 3.0)
            futures.append(ex.submit(worker, c, t))
        # aguarda fim
        for f in futures:
            f.result()

    print("Todos os clientes terminaram.")
