####################################################################################################
#
# Modelo de API do primeiro desafio com perfil Back-end: Api Simulador
# 
# Autor: Rômulo Alves c153824
#
# Desenvolvida utilizando Python 3.11, sendo também necessária a instalação do Microsoft ODBC Driver
# versão 18 e os módulos pip: fastapi, pyodbc e azure-eventhub
#
#####################################################################################################

import pyodbc 
import json
import configparser
from typing import List, Dict
from azure.eventhub import EventData
from azure.eventhub.exceptions import EventHubError
from azure.eventhub.aio import EventHubProducerClient
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# Cria objeto com as configurações do banco de dados e Event Hub
config = configparser.ConfigParser()
config.read('config.ini')

# Definindo um modelo para o corpo da requisição
class Simulacao(BaseModel):
    valorDesejado: float
    prazo: int

# Criando a aplicação FastAPI
app = FastAPI()

# Rota para executar uma simulação de empréstimo
@app.post("/simulacao")
async def simulacao(request: Simulacao):
    
    # Extrai os parametros do request
    valorDesejado = request.valorDesejado
    prazo = request.prazo
    
    # Cria a conexão com o banco
    conn = await criaConexaoBanco()
        
    try:
        with conn.cursor() as cursor:
            
            # Constroi a consulta ao banco para retornar o produto que se enquadra no perfil do cliente que está simulando um empréstimo
            query = f"SELECT CO_PRODUTO AS codigoProduto, NO_PRODUTO AS descricaoProduto, CAST(PC_TAXA_JUROS AS Float) AS taxaJuros FROM dbo.Produto WHERE ({prazo} BETWEEN NU_MINIMO_MESES AND NU_MAXIMO_MESES) AND ({valorDesejado} BETWEEN VR_MINIMO AND VR_MAXIMO)"
    
            # Executa a consulta
            cursor.execute(query)
    
            # Pega todas as linhas do resultado e o nome de todas as colunas
            rows = cursor.fetchall()
            column_names = [column[0] for column in cursor.description]
        
            # Prepara a resposta da API baseda nos resultados da consulta
            if rows:
                for row in rows:    
                    # Monta dicionário com código, descrição e taxa de juros do produto
                    respostaSimulacao = dict(zip(column_names, row))
                # Executa a simulação de empréstimo
                resultadoSimulacao = simularEmprestimo(valorDesejado,float(respostaSimulacao["taxaJuros"]),prazo)
                # Inclui o resultado no dicionário
                respostaSimulacao["resultadoSimulacao"] = resultadoSimulacao
                # Envia o evento ao Event Hub
                await enviarEvento(json.dumps(respostaSimulacao))
                # Retorna o envelope enviado ao Event Hub
                return JSONResponse(content=respostaSimulacao)
            else:
                return JSONResponse(content={"detalhe":"Nenhum produto encontrado baseado nas informações fornecidas."})
    finally:
        conn.close()


# Funcão para simular empréstimo no modelo SAC e PRICE
def simularEmprestimo(valorDesejado: float, valorJuros: float, prazo: int) -> List[Dict]:
    resultadoSimulacao = []

    #Modelo SAC
    parcelasSac = calcularParcelasSac(valorDesejado, valorJuros, prazo)
    resultadoSimulacao.append({"tipo": "SAC", "parcelas": parcelasSac})

    #Modelo PRICE
    parcelasPrice = calcularParcelasPrice(valorDesejado, valorJuros, prazo)
    resultadoSimulacao.append({"tipo": "PRICE", "parcelas": parcelasPrice})

    return resultadoSimulacao

# Função para calcular as parcelas em um modelo SAC
def calcularParcelasSac(valorDesejado: float, valorJuros: float, prazo: int) -> List[Dict]:
    amortizacaoSac = valorDesejado / prazo
    saldoDevedorSac = valorDesejado
    parcelasSac = []

    for i in range(prazo):
        jurosSac = saldoDevedorSac * valorJuros
        saldoDevedorSac -= amortizacaoSac
        valorParcelaSac = amortizacaoSac + jurosSac

        parcela = {
            "numero": i + 1,
            "valorAmortizacao": round(amortizacaoSac,2),
            "valorJuros": round(jurosSac,2),
            "valorPrestacao": round(valorParcelaSac,2)
        }
        parcelasSac.append(parcela)

    return parcelasSac

# Função para calcular as parcelas em um modelo Price
def calcularParcelasPrice(valorDesejado: float, valorJuros: float, prazo: int) -> List[Dict]:
    valorParcelaPrice = (valorDesejado * valorJuros) / (1 - (1 + valorJuros) ** -prazo)
    parcelasPrice = []

    for i in range(prazo):
        parcela = {
            "numero": i + 1,
            "valorAmortizacao": round(valorParcelaPrice - (valorDesejado * valorJuros), 2),
            "valorJuros": round(valorDesejado * valorJuros, 2),
            "valorPrestacao": round(valorParcelaPrice, 2)
        }
        parcelasPrice.append(parcela)

    return parcelasPrice




#Função para enviar um evento a um Event Hub
async def enviarEvento(evento: json):
    #Cria um client para enviar mensagens para o Event Hub
    producer = EventHubProducerClient.from_connection_string(
        conn_str=config['event_hub_config']['connection_str'], eventhub_name=config['event_hub_config']['name']
    )
    async with producer:
        # Cria um batch
        event_data_batch = await producer.create_batch()
        # Adiciona eventos ao batch
        event_data_batch.add(EventData(evento))
        
        try:
        # Envia os eventos para o Event Hub
            await producer.send_batch(event_data_batch)
        except EventHubError as eh_err:
            raise HTTPException(status_code=500, detail='Erro enviando envelope ao Event Hub: {eh_err}')
            
   

#Cria a conexão com o banco de dados        
async def criaConexaoBanco():
    conn_str = f"""DRIVER={config['database_config']['driver']};SERVER={config['database_config']['host']};DATABASE={config['database_config']['database']};UID={config['database_config']['user']};PWD={config['database_config']['password']}"""
    
    try:
        conn = pyodbc.connect(conn_str)
        return conn
    except pyodbc.Error as e:
        raise HTTPException(status_code=500, detail='Erro conectando a base: {e}')
    
    
    

