import json
import time
import copy

class TCB:
    def __init__(self, tid, pid, arrival_time, cpu_time, priority):
        self.tid = tid
        self.pid = pid
        self.arrival_time = arrival_time
        self.cpu_time = cpu_time
        self.priority = priority
        self.state = "Ready"
        
        # Métricas de controle
        self.remaining_time = cpu_time
        self.start_time = None
        self.end_time = None
        self.waiting_time = 0
        self.response_time = None

    def __repr__(self):
        return f"Thread({self.tid}, State: {self.state}, Rem: {self.remaining_time})"

class PCB:
    def __init__(self, pid, arrival_time, cpu_time, priority, threads_data):
        self.pid = pid
        self.arrival_time = arrival_time
        self.cpu_time = cpu_time
        self.priority = priority
        self.state = "Ready"
        self.threads_count = len(threads_data)
        
        # Inicializa as TCBs associadas
        self.threads = [
            TCB(t['tid'], pid, t['arrival_time'], t['cpu_time'], t['priority'])
            for t in threads_data
        ]
        
        # Métricas de controle do Processo
        self.remaining_time = sum(t.remaining_time for t in self.threads)
        self.start_time = None
        self.end_time = None
        self.waiting_time = 0

    def __repr__(self):
        return f"Process({self.pid}, Threads: {len(self.threads)}, State: {self.state})"

def ler_json(json_str):
    with open(json_str, 'r') as f:
        data = json.loads(f.read())
        processos = []
        for p in data['processes']:
            proc = PCB(p['pid'], p['arrival_time'], p['cpu_time'], p['priority'], p['threads'])
            processos.append(proc)
        return processos

# ---------------- MÉTRICAS ----------------
def calcular_metricas(processos_finalizados, ordem_execucao, clock, trocas_contexto=0):
    n = len(processos_finalizados)

    if n == 0:
        return {
            "ordem": "",
            "espera_media": 0,
            "resposta_media": 0,
            "turnaround_medio": 0,
            "throughput": 0,
            "trocas_contexto": 0,
            "tempo_simulacao": clock
        }

    total_turnaround = 0
    total_espera = 0
    total_resposta = 0

    for p in processos_finalizados:

        turnaround = p.end_time - p.arrival_time

        burst = sum(t.cpu_time for t in p.threads)

        espera = turnaround - burst

        resposta = p.start_time - p.arrival_time

        total_turnaround += turnaround
        total_espera += espera
        total_resposta += resposta

    return {
        "ordem": " -> ".join(ordem_execucao),
        "espera_media": total_espera / n,
        "resposta_media": total_resposta / n,
        "turnaround_medio": total_turnaround / n,
        "throughput": n / clock if clock else 0,
        "trocas_contexto": trocas_contexto,
        "tempo_simulacao": clock
    }

# ---------------- FCFS ----------------
def simular_fcfs(processos_originais):
    processos = copy.deepcopy(processos_originais)
    clock = 0
    ordem_execucao = []
    fila_prontos = []
    processos_finalizados = []
    trocas_contexto = 0
    ultimo_tid = None
    
    while processos or fila_prontos:
        chegadas = [p for p in processos if p.arrival_time <= clock]
        for p in chegadas:
            fila_prontos.append(p)
            processos.remove(p)
        
        if not fila_prontos:
            clock += 1
            continue
        
        processo_atual = fila_prontos[0]
        if processo_atual.start_time is None:
            processo_atual.start_time = clock
        processo_atual.state = "Running"
        
        threads_prontas = [t for t in processo_atual.threads if t.remaining_time > 0 and t.arrival_time <= clock]
        if not threads_prontas:
            clock += 1
            continue
        
        thread_atual = min(
    threads_prontas,
    key=lambda t: (t.arrival_time, t.tid)
)
        if ultimo_tid is not None and ultimo_tid != thread_atual.tid:
            trocas_contexto += 1
        ultimo_tid = thread_atual.tid
        
        if thread_atual.start_time is None:
            thread_atual.start_time = clock
            thread_atual.response_time = clock - thread_atual.arrival_time
            thread_atual.state = "Running"
        
        ordem_execucao.append(f"{processo_atual.pid}({thread_atual.tid})")
        
        # Contabiliza tempo de espera para as outras threads/processos na fila
        for p in fila_prontos:
            if p != processo_atual:
                pass
        
        thread_atual.remaining_time -= 1
        processo_atual.remaining_time -= 1
        clock += 1
        
        if thread_atual.remaining_time == 0:
            thread_atual.end_time = clock
            thread_atual.state = "Terminated"
        else:
            thread_atual.state = "Ready"
        if all(t.remaining_time == 0 for t in processo_atual.threads):
            processo_atual.end_time = clock
            processo_atual.state = "Terminated"
            fila_prontos.remove(processo_atual)
            processos_finalizados.append(processo_atual)
        else:
            processo_atual.state = "Ready"
    return calcular_metricas(processos_finalizados, ordem_execucao, clock, trocas_contexto)

# ---------------- SJF ----------------
def simular_sjf(processos_originais):
    processos = copy.deepcopy(processos_originais)
    clock = 0
    ordem_execucao = []
    fila_prontos = []
    processos_finalizados = []
    trocas_contexto = 0
    ultimo_tid = None
    
    while processos or fila_prontos:
        chegadas = [p for p in processos if p.arrival_time <= clock]
        for p in chegadas:
            fila_prontos.append(p)
            processos.remove(p)
        
        if not fila_prontos:
            clock += 1
            continue
        
        processo_atual = min(fila_prontos, key=lambda p: p.remaining_time)
        if processo_atual.start_time is None:
            processo_atual.start_time = clock
        processo_atual.state = "Running"
            
        threads_prontas = [t for t in processo_atual.threads if t.remaining_time > 0 and t.arrival_time <= clock]
        if not threads_prontas:
            clock += 1
            continue
        
        thread_atual = min(
            threads_prontas,
            key=lambda t: (t.remaining_time, t.arrival_time)
        )
        if ultimo_tid is not None and ultimo_tid != thread_atual.tid:
            trocas_contexto += 1
        ultimo_tid = thread_atual.tid

        if thread_atual.start_time is None:
            thread_atual.start_time = clock
            thread_atual.response_time = clock - thread_atual.arrival_time
            thread_atual.state = "Running"

        ordem_execucao.append(f"{processo_atual.pid}({thread_atual.tid})")
        
        for p in fila_prontos:
            if p != processo_atual:
                pass

        thread_atual.remaining_time -= 1
        processo_atual.remaining_time -= 1
        clock += 1
        
        if thread_atual.remaining_time == 0:
            thread_atual.end_time = clock
            thread_atual.state = "Terminated"
        else:
            thread_atual.state = "Ready"
        if all(t.remaining_time == 0 for t in processo_atual.threads):
            processo_atual.end_time = clock
            processo_atual.state = "Terminated"
            fila_prontos.remove(processo_atual)
            processos_finalizados.append(processo_atual)
        else:
            processo_atual.state = "Ready"
    return calcular_metricas(processos_finalizados, ordem_execucao, clock, trocas_contexto)

# ---------------- PRIORIDADE ----------------
def simular_prioridade(processos_originais):
    processos = copy.deepcopy(processos_originais)
    clock = 0
    ordem_execucao = []
    fila_prontos = []
    processos_finalizados = []
    trocas_contexto = 0
    ultimo_tid = None
    
    while processos or fila_prontos:
        chegadas = [p for p in processos if p.arrival_time <= clock]
        for p in chegadas:
            fila_prontos.append(p)
            processos.remove(p)
        
        if not fila_prontos:
            clock += 1
            continue
        
        processo_atual = min(fila_prontos, key=lambda p: p.priority)
        if processo_atual.start_time is None:
            processo_atual.start_time = clock
        processo_atual.state = "Running"

        threads_prontas = [t for t in processo_atual.threads if t.remaining_time > 0 and t.arrival_time <= clock]
        if not threads_prontas:
            clock += 1
            continue
        
        thread_atual = min(
            threads_prontas,
            key=lambda t: (t.priority, t.arrival_time)
        )
        if ultimo_tid is not None and ultimo_tid != thread_atual.tid:
            trocas_contexto += 1
        ultimo_tid = thread_atual.tid

        if thread_atual.start_time is None:
            thread_atual.start_time = clock
            thread_atual.response_time = clock - thread_atual.arrival_time
            thread_atual.state = "Running"

        ordem_execucao.append(f"{processo_atual.pid}({thread_atual.tid})")
        
        for p in fila_prontos:
            if p != processo_atual:
                pass

        thread_atual.remaining_time -= 1
        processo_atual.remaining_time -= 1
        clock += 1
        
        if thread_atual.remaining_time == 0:
            thread_atual.end_time = clock
            thread_atual.state = "Terminated"
        else:
            thread_atual.state = "Ready"
        if all(t.remaining_time == 0 for t in processo_atual.threads):
            processo_atual.end_time = clock
            processo_atual.state = "Terminated"
            fila_prontos.remove(processo_atual)
            processos_finalizados.append(processo_atual)
        else:
            processo_atual.state = "Ready"
    
    return calcular_metricas(processos_finalizados, ordem_execucao, clock, trocas_contexto)

# ---------------- ROUND ROBIN ----------------
def simular_round_robin(processos_originais, quantum=2):
    processos = copy.deepcopy(processos_originais)
    clock = 0
    ordem_execucao = []
    fila_prontos = []
    processos_finalizados = []
    trocas_contexto = 0
    ultimo_tid = None
    
    while processos or fila_prontos:
        chegadas = [p for p in processos if p.arrival_time <= clock]
        for p in chegadas:
            fila_prontos.append(p)
            processos.remove(p)
        
        if not fila_prontos:
            clock += 1
            continue
        
        processo_atual = fila_prontos.pop(0)
        if processo_atual.start_time is None:
            processo_atual.start_time = clock
        processo_atual.state = "Running"

        threads_prontas = [t for t in processo_atual.threads if t.remaining_time > 0 and t.arrival_time <= clock]
        if not threads_prontas:
            thread_atual.state = "Ready"
            processo_atual.state = "Ready"
            fila_prontos.append(processo_atual)
            clock += 1
            continue
        
        thread_atual = min(
            threads_prontas,
            key=lambda t: (t.arrival_time, t.tid)
        )
        if ultimo_tid is not None and ultimo_tid != thread_atual.tid:
            trocas_contexto += 1
        ultimo_tid = thread_atual.tid

        if thread_atual.start_time is None:
            thread_atual.start_time = clock
            thread_atual.response_time = clock - thread_atual.arrival_time
            thread_atual.state = "Running"

        tempo_exec = min(quantum, thread_atual.remaining_time)
        
        for _ in range(tempo_exec):
            ordem_execucao.append(f"{processo_atual.pid}({thread_atual.tid})")
            
            for p in fila_prontos:
                pass
                
            thread_atual.remaining_time -= 1
            processo_atual.remaining_time -= 1
            clock += 1
            
            # Checa novas chegadas durante o decorrer do quantum
            novas_chegadas = [p for p in processos if p.arrival_time <= clock]
            for p in novas_chegadas:
                fila_prontos.append(p)
                processos.remove(p)
        
        if thread_atual.remaining_time == 0:
            thread_atual.end_time = clock
            thread_atual.state = "Terminated"
        else:
            thread_atual.state = "Ready"
        if all(t.remaining_time == 0 for t in processo_atual.threads):
            processo_atual.end_time = clock
            processo_atual.state = "Terminated"
            processos_finalizados.append(processo_atual)
        else:
            thread_atual.state = "Ready"
            processo_atual.state = "Ready"
            fila_prontos.append(processo_atual)
    
    return calcular_metricas(processos_finalizados, ordem_execucao, clock, trocas_contexto)

# ---------------- MÚLTIPLAS FILAS ----------------
def simular_multiplas_filas(processos_originais, quantum=2):
    processos = copy.deepcopy(processos_originais)
    clock = 0
    ordem_execucao = []
    fila_alta = []
    fila_baixa = []
    processos_finalizados = []
    trocas_contexto = 0
    ultimo_tid = None
    
    while processos or fila_alta or fila_baixa:
        chegadas = [p for p in processos if p.arrival_time <= clock]
        for p in chegadas:
            if p.priority <= 2:   # prioridade alta
                fila_alta.append(p)
            else:
                fila_baixa.append(p)
            processos.remove(p)
        
        if fila_alta:
            processo_atual = fila_alta[0]
            if processo_atual.start_time is None:
                processo_atual.start_time = clock
            processo_atual.state = "Running"

            threads_prontas = [t for t in processo_atual.threads if t.remaining_time > 0 and t.arrival_time <= clock]
            if not threads_prontas:
                clock += 1
                continue

            thread_atual = min(
                threads_prontas,
                key=lambda t: (t.arrival_time, t.tid)
            )
            if ultimo_tid is not None and ultimo_tid != thread_atual.tid:
                trocas_contexto += 1
            ultimo_tid = thread_atual.tid

            if thread_atual.start_time is None:
                thread_atual.start_time = clock
                thread_atual.response_time = clock - thread_atual.arrival_time
                thread_atual.state = "Running"

            ordem_execucao.append(f"{processo_atual.pid}({thread_atual.tid})")
            
            for p in fila_alta:
                if p != processo_atual: pass
            for p in fila_baixa:
                pass

            thread_atual.remaining_time -= 1
            processo_atual.remaining_time -= 1
            clock += 1
            
            if thread_atual.remaining_time == 0:
                thread_atual.end_time = clock
                thread_atual.state = "Terminated"
            else:
                thread_atual.state = "Ready"
            if all(t.remaining_time == 0 for t in processo_atual.threads):
                processo_atual.end_time = clock
                processo_atual.state = "Terminated"
                fila_alta.remove(processo_atual)
                processos_finalizados.append(processo_atual)
            else:
                processo_atual.state = "Ready"
        
        elif fila_baixa:
            processo_atual = fila_baixa.pop(0)
            if processo_atual.start_time is None:
                processo_atual.start_time = clock
            processo_atual.state = "Running"

            threads_prontas = [t for t in processo_atual.threads if t.remaining_time > 0 and t.arrival_time <= clock]
            if not threads_prontas:
                fila_baixa.append(processo_atual)
                clock += 1
                continue

            thread_atual = min(
    threads_prontas,
    key=lambda t: (t.arrival_time, t.tid)
)
            if ultimo_tid is not None and ultimo_tid != thread_atual.tid:
                trocas_contexto += 1
            ultimo_tid = thread_atual.tid

            if thread_atual.start_time is None:
                thread_atual.start_time = clock
                thread_atual.response_time = clock - thread_atual.arrival_time
                thread_atual.state = "Running"

            tempo_exec = min(quantum, thread_atual.remaining_time)
            for _ in range(tempo_exec):
                ordem_execucao.append(f"{processo_atual.pid}({thread_atual.tid})")
                
                for p in fila_alta:
                    pass
                for p in fila_baixa:
                    pass
                
                thread_atual.remaining_time -= 1
                processo_atual.remaining_time -= 1
                clock += 1
                
                # Checa novas chegadas para interromper ou popular filas
                novas_chegadas = [p for p in processos if p.arrival_time <= clock]
                for p in novas_chegadas:
                    if p.priority <= 2:
                        fila_alta.append(p)
                    else:
                        fila_baixa.append(p)
                    processos.remove(p)

            if thread_atual.remaining_time == 0:
                thread_atual.end_time = clock
                thread_atual.state = "Terminated"
            else:
                thread_atual.state = "Ready"
            if all(t.remaining_time == 0 for t in processo_atual.threads):
                processo_atual.end_time = clock
                processo_atual.state = "Terminated"
                processos_finalizados.append(processo_atual)
            else:
                fila_baixa.append(processo_atual)
        else:
            clock += 1
    
    return calcular_metricas(processos_finalizados, ordem_execucao, clock, trocas_contexto)


if __name__ == '__main__':    
    print("=== INICIANDO BENCHMARK ===")
    start_bench = time.perf_counter()
    
    # Certifique-se de que o arquivo dados.json existe no diretório correto.
    try:
        processos = ler_json("dados.json")
    except FileNotFoundError:
        print("Erro: O arquivo 'dados.json' não foi encontrado.")
        exit(1)
    
    algoritmos = {
        "FCFS": simular_fcfs,
        "SJF": simular_sjf,
        "Prioridade": simular_prioridade,
        "Round Robin": lambda p: simular_round_robin(p, quantum=2),
        "Múltiplas Filas": lambda p: simular_multiplas_filas(p, quantum=2)
    }
    
    for nome, func in algoritmos.items():
        print(f"\n=== {nome} ===")
        res = func(processos)
        print("[Ordem de Execução]")
        print(res["ordem"])
        print("\n[Métricas Obtidas]")
        print(f"Tempo médio de espera: {res['espera_media']:.1f}")
        print(f"Tempo médio de resposta: {res['resposta_media']:.1f}")
        print(f"Turnaround médio: {res['turnaround_medio']:.1f}")
        print(f"Throughput: {res['throughput']:.3f}")
        print(f"Trocas de contexto: {res['trocas_contexto']}")
        print(f"Tempo total da simulação: {res['tempo_simulacao']}")
    
    end_bench = time.perf_counter()
    bench_time_ms = (end_bench - start_bench) * 1000
    print(f"\nTempo de execução do benchmark: {bench_time_ms:.4f} ms")
