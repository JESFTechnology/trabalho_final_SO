# -*- coding: utf-8 -*-
"""
Trabalho Prático - Benchmark de Gerência de Processos e Threads
Disciplina: Sistemas Operacionais
"""

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
            processos = proc
        return processos

def simular_fcfs_dois_niveis(processos_originais):
    # Clona os processos para não afetar os originais
    processos = copy.deepcopy(processos_originais)
    
    clock = 0
    ordem_execucao = []
    trocas_contexto = 0
    ultimo_tid_executado = None
    
    fila_processos_prontos = []
    processos_finalizados = []
    
    # Loop de simulação por clock discreto
    while processos or fila_processos_prontos:
        # 1. Admissão de Processos que chegaram no clock atual
        chegadas = [p for p in processos if p.arrival_time == clock]
        for p in chegadas:
            fila_processos_prontos.append(p)
            processos.remove(p)
            
        if not fila_processos_prontos:
            clock += 1
            continue
            
        # 2. Escalonador de Processo (FCFS: Primeiro que chegou)
        # Ordena por tempo de chegada se necessário, mas mantemos a ordem da fila FCFS
        processo_atual = fila_processos_prontos[0]
        
        if processo_atual.start_time is None:
            processo_atual.start_time = clock
            processo_atual.state = "Running"
            
        # 3. Escalonador de Thread (FCFS dentro do processo)
        threads_prontas = [t for t in processo_atual.threads if t.remaining_time > 0 and t.arrival_time <= clock]
        
        if not threads_prontas:
            # Se não há threads prontas deste processo que possam rodar agora, avançamos o clock
            clock += 1
            continue
            
        # FCFS seleciona a primeira thread não finalizada
        thread_atual = threads_prontas[0]
        
        # Contabiliza Troca de Contexto
        if ultimo_tid_executado is not None and ultimo_tid_executado != thread_atual.tid:
            trocas_contexto += 1
            
        ultimo_tid_executado = thread_atual.tid
        
        # Registra tempos da thread
        if thread_atual.start_time is None:
            thread_atual.start_time = clock
            thread_atual.response_time = clock - thread_atual.arrival_time
            thread_atual.state = "Running"
            
        # Executa por 1 unidade de tempo (Clock Discreto)
        ordem_execucao.append(f"{processo_atual.pid}({thread_atual.tid})")
        thread_atual.remaining_time -= 1
        processo_atual.remaining_time -= 1
        clock += 1
        
        # Atualiza o tempo de espera das demais threads e processos na fila
        for p in fila_processos_prontos:
            if p != processo_atual and p.arrival_time < clock:
                p.waiting_time += 1
            for t in p.threads:
                if t != thread_atual and t.remaining_time > 0 and t.arrival_time < clock:
                    t.waiting_time += 1

        # Verifica se a thread terminou
        if thread_atual.remaining_time == 0:
            thread_atual.end_time = clock
            thread_atual.state = "Terminated"
            
        # Verifica se o processo terminou todas as suas threads
        if all(t.remaining_time == 0 for t in processo_atual.threads):
            processo_atual.end_time = clock
            processo_atual.state = "Terminated"
            processos_finalizados.append(processo_atual)
            fila_processos_prontos.remove(processo_atual)
            
    # Cálculo das Métricas Finais
    total_turnaround_p = sum((p.end_time - p.arrival_time) for p in processos_finalizados)
    total_espera_p = sum(p.waiting_time for p in processos_finalizados)
    
    # Como resposta do processo depende da primeira thread:
    total_resposta_p = sum((p.start_time - p.arrival_time) for p in processos_finalizados)
    
    n_proc = len(processos_finalizados)
    
    metricas = {
        "ordem": " -> ".join(ordem_execucao),
        "espera_media": total_espera_p / n_proc,
        "resposta_media": total_resposta_p / n_proc,
        "turnaround_medio": total_turnaround_p / n_proc,
        "throughput": n_proc / clock,
        "trocas_contexto": trocas_contexto,
        "tempo_simulacao": clock
    }
    
    return metricas

if __name__ == '__main__':    
    print("=== INICIANDO BENCHMARK ===")
    start_bench = time.perf_counter()
    
    processos = ler_json("dados.json")
    res = simular_fcfs_dois_niveis(processos)
    
    end_bench = time.perf_counter()
    bench_time_ms = (end_bench - start_bench) * 1000
    
    print("\n[Ordem de Execução]")
    print(res["ordem"])
    
    print("\n[Métricas Obtidas]")
    print(f"Tempo médio de espera: {res['espera_media']:.1f} unidades")
    print(f"Tempo médio de resposta: {res['resposta_media']:.1f} unidades")
    print(f"Turnaround médio: {res['turnaround_medio']:.1f} unidades")
    print(f"Throughput: {res['throughput']:.3f} processos/unidade de tempo")
    print(f"Trocas de contexto: {res['trocas_contexto']}")
    print(f"Tempo total da simulação: {res['tempo_simulacao']} unidades")
    print(f"Tempo de execução do benchmark: {bench_time_ms:.4f} ms")