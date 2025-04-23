# Ingestão de Dados Transacionais em Batch via Airflow

Este projeto realiza a ingestão de dados transacionais particionados por dia com o intuito de simular uma arquitetura em camadas (medalhão): **source → bronze → silver**. 

Todo o pipeline é executado localmente, orquestrado por uma instância do **Apache Airflow**, e utilizando **Apache Spark** com suporte ao **Delta Lake**.

>>OBS: foi escolhido PySpark por questões didáticas. Em um contexto real, a massa de dados precisaria ser maior para justificar a necessidade da tecnologia. Mas, para utilização junto ao Delta, o PySpark foi a escolha mais simples.

---

## 🗂 Estrutura do Projeto

```
project-root/
│
├── mock_data/
│   └── MOCK_DATA.csv              # Base de dados completa usada como origem
│
├── datalake/                      # Pasta externa contendo os dados organizados por camadas. Você precisa criar essa pasta e apontar na variável "DATALAKE_PATH" dentro dos arquivos
│   ├── source/
│   │   └── transaction_system/    # Arquivos CSV diários de entrada
│   ├── bronze/
│   │   └── transaction_data/      # Dados no formato Delta Lake (raw zone)
│   └── silver/
│       └── transaction_data/      # Dados tratados e deduplicados no formato Delta Lake
│
├── dags/
│   └── ingest_transaction_data.py # DAG do Airflow responsável pela ingestão diária. Você pode utilizar o comando 'cp' para copiar essa DAG para sua pasta de DAGs do Airflow.
│
├── scripts/
│   ├── mock_data_spliter.py       # Divide o MOCK_DATA.csv em 10 partes com datas diferentes
│   ├── ingest_source_to_bronze.py # Realiza ingestão dos arquivos CSV para camada bronze
│   └── ingest_bronze_to_silver.py # Atualiza a camada silver com dados incrementais da bronze
│
├── requirements.txt               # Bibliotecas necessárias para execução
└── README.md                      # Este arquivo
```

> ⚠️ Certifique-se de criar manualmente a pasta `datalake/` na raiz do projeto antes da execução e apontar o caminho absoluto nas respectivas variáveis.

---

## ⚙️ Como Executar

### 1. Instale as dependências
```bash
pip install -r requirements.txt
```

### 2. Gere os arquivos simulados
```bash
python scripts/mock_data_spliter.py
```

### 3. Inicie o Airflow standalone
```bash
airflow standalone
```

### 4. Ative a DAG no painel do Airflow
Acesse o Airflow em [http://localhost:8080](http://localhost:8080) e ative a DAG `INGEST_TRANSACTION_DATA`.

> A DAG está agendada para rodar todos os dias às 03:00 (`0 3 * * *`), com suporte a **catchup** de dias anteriores. A simulação inclui falhas nos dias em que não existem arquivos.

---

---

## 📦 Tecnologias Utilizadas

- **Apache Spark** (Standalone)
- **Delta Lake**
- **Apache Airflow** (Standalone)
- **Python 3.10+**
- **Pandas**

---

## 📄 Licença

Este projeto está licenciado sob a licença **MIT** e é livre para reprodução e customização.

---

## 📬 Contato

- [LinkedIn](https://www.linkedin.com/in/marco-caja)  
- [Instagram](https://www.instagram.com/omarcocaja)

---

