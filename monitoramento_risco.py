import paho.mqtt.client as mqtt
import pymysql
from datetime import datetime
import json
import heapq

# Estruturas auxiliares

# --- Lista ligada simples para sensores ---
class NodeSensor:
    def __init__(self, sensor_id, regiao_id):
        self.sensor_id = sensor_id
        self.regiao_id = regiao_id
        self.next = None

class ListaLigadaSensores:
    def __init__(self):
        self.head = None

    def adicionar(self, sensor_id, regiao_id):
        novo = NodeSensor(sensor_id, regiao_id)
        if not self.head:
            self.head = novo
        else:
            atual = self.head
            while atual.next:
                atual = atual.next
            atual.next = novo

    def existe(self, sensor_id):
        atual = self.head
        while atual:
            if atual.sensor_id == sensor_id:
                return True
            atual = atual.next
        return False

# --- Árvore binária para medições (BST por valor) ---
class NodeMedicao:
    def __init__(self, valor, tipo):
        self.valor = valor
        self.tipo = tipo
        self.left = None
        self.right = None

class ArvoreMedicoes:
    def __init__(self):
        self.root = None

    def inserir(self, valor, tipo):
        self.root = self._inserir(self.root, valor, tipo)

    def _inserir(self, node, valor, tipo):
        if node is None:
            return NodeMedicao(valor, tipo)
        if valor < node.valor:
            node.left = self._inserir(node.left, valor, tipo)
        else:
            node.right = self._inserir(node.right, valor, tipo)
        return node

    def buscar(self, valor):
        return self._buscar(self.root, valor)

    def _buscar(self, node, valor):
        if node is None:
            return None
        if node.valor == valor:
            return node
        elif valor < node.valor:
            return self._buscar(node.left, valor)
        else:
            return self._buscar(node.right, valor)

# --- Fila para medições (FIFO) ---
class Fila:
    def __init__(self):
        self.itens = []

    def enfileirar(self, item):
        self.itens.append(item)

    def desenfileirar(self):
        if self.esta_vazia():
            return None
        return self.itens.pop(0)

    def esta_vazia(self):
        return len(self.itens) == 0

# --- Pilha para alertas (LIFO) ---
class Pilha:
    def __init__(self):
        self.itens = []

    def empilhar(self, item):
        self.itens.append(item)

    def desempilhar(self):
        if self.esta_vazia():
            return None
        return self.itens.pop()

    def esta_vazia(self):
        return len(self.itens) == 0

# --- Heap para alertas por nível de risco ---
import heapq
class HeapAlertas:
    def __init__(self):
        self.heap = []

    def adicionar(self, nivel_risco, alerta):
        heapq.heappush(self.heap, (nivel_risco, alerta))

    def pegar_maior(self):
        if not self.heap:
            return None
        nivel_risco_neg, alerta = self.heap[0]
        return (-nivel_risco_neg, alerta)

    def remover_maior(self):
        if not self.heap:
            return None
        nivel_risco_neg, alerta = heapq.heappop(self.heap)
        return (-nivel_risco_neg, alerta)

# ---------------------------------------------------------
# Conexão com banco de dados MySQL
import pymysql

db = pymysql.connect(
    host='localhost',
    port=4343,
    user='admin_gustavo',
    password='@SAVEFOREST1',
    database='sfgb'
)
cursor = db.cursor()

def inserir_medicao(valor, sensor_id, tipo_medicao):
    # Limita 1 registro de cada tipo por sensor (deleta os antigos antes)
    delete_sql = """
    DELETE FROM MEDICAO WHERE SENSOR_ID_SENSOR = %s AND TIPO_MEDICAO = %s
    """
    cursor.execute(delete_sql, (sensor_id, tipo_medicao))
    db.commit()

    sql = """
    INSERT INTO MEDICAO (DATA_HORA_MEDICAO, VALOR_MEDICAO, SENSOR_ID_SENSOR, TIPO_MEDICAO)
    VALUES (%s, %s, %s, %s)
    """
    agora = datetime.now()
    cursor.execute(sql, (agora, valor, sensor_id, tipo_medicao))
    db.commit()

def inserir_alerta(nivel_risco, mensagem, regiao_id):
    # Mantém 1 alerta por região, deleta alertas antigos da mesma região antes
    delete_sql = """
    DELETE FROM ALERTA WHERE REGIAO_ID_REGIAO = %s
    """
    cursor.execute(delete_sql, (regiao_id,))
    db.commit()

    sql = """
    INSERT INTO ALERTA (NIVEL_RISCO, DATA_HORA_ALERTA, MENSAGEM_ALERTA, REGIAO_ID_REGIAO)
    VALUES (%s, %s, %s, %s)
    """
    agora = datetime.now()
    cursor.execute(sql, (nivel_risco, agora, mensagem, regiao_id))
    db.commit()

# Avaliação de risco simplificada
def avaliar_risco(fumaca, temp, umidade):
    risco = 0
    if fumaca > 60:
        risco += 50
    if temp > 35:
        risco += 30
    if umidade < 30:
        risco += 20
    
    if risco == 0:
        return "Sem risco"
    elif risco  <= 40:
        return "Baixo"
    elif risco <= 70:
        return "Médio"
    else:
        return "Alto"

# Instancia das estruturas
fila_medicoes = Fila()
pilha_alertas = Pilha()
lista_sensores = ListaLigadaSensores()
arvore_medicoes = ArvoreMedicoes()
heap_alertas = HeapAlertas()

def processar_medicoes():
    while not fila_medicoes.esta_vazia():
        medicao = fila_medicoes.desenfileirar()
        valor = medicao['valor']
        sensor_id = medicao['sensor_id']
        tipo = medicao['tipo']
        regiao_id = medicao['regiao_id']

        if not lista_sensores.existe(sensor_id):
            lista_sensores.adicionar(sensor_id, regiao_id)

        arvore_medicoes.inserir(valor, tipo)
        inserir_medicao(valor, sensor_id, tipo)

def processar_alertas(nivel_risco, mensagem, regiao_id):
    pilha_alertas.empilhar({'nivel': nivel_risco, 'mensagem': mensagem, 'regiao_id': regiao_id})
    heap_alertas.adicionar(nivel_risco, mensagem)
    inserir_alerta(nivel_risco, mensagem, regiao_id)

def on_message(client, userdata, msg):
    dados = json.loads(msg.payload)
    temp = float(dados.get("temp", 0))
    umidade = float(dados.get("umidade", 0))
    fumaca = float(dados.get("fumaca", 0))
    sensor_id = int(dados.get("sensor_id", 0))
    regiao_id = int(dados.get("regiao_id", 0))

    fila_medicoes.enfileirar({'valor': temp, 'sensor_id': sensor_id, 'tipo': 'temperatura', 'regiao_id': regiao_id})
    fila_medicoes.enfileirar({'valor': umidade, 'sensor_id': sensor_id, 'tipo': 'umidade', 'regiao_id': regiao_id})
    fila_medicoes.enfileirar({'valor': fumaca, 'sensor_id': sensor_id, 'tipo': 'fumaca', 'regiao_id': regiao_id})

    processar_medicoes()

    nivel_risco = avaliar_risco(fumaca, temp, umidade)
    print(f"Nível de risco: {nivel_risco}")

    if nivel_risco != "Sem risco":
        mensagem = f"Alerta de risco de queimada! Nível: {nivel_risco}"
        processar_alertas(nivel_risco, mensagem, regiao_id)

# Config MQTT
client = mqtt.Client()
client.on_message = on_message

client.connect("localhost", 14883, 60)

# Aqui a mudança para escutar todos os sensores no padrão deviceXXX
client.subscribe("/TEF/device/+")

print("Escutando o tópico MQTT para múltiplos sensores...")

client.loop_forever()
