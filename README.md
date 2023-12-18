# Projeto de Banco de Dados de Atletas Olímpicos

Este projeto consiste em um script Python que cria um banco de dados PostgreSQL e o popula com dados sobre atletas, equipes, esportes e medalhas olímpicas.

## Descrição

O script Python utiliza a biblioteca `psycopg2` para estabelecer uma conexão com um banco de dados PostgreSQL e executa várias operações SQL. O objetivo é criar tabelas para armazenar dados sobre regiões (países), times, atletas, esportes, modalidades e medalhas olímpicas, importando esses dados de arquivos CSV.

## Pré-requisitos

Antes de executar o script, certifique-se de ter instalado:

- Python (3.x ou superior)
- PostgreSQL
- psycopg2

## Configuração

1. **Configurar o PostgreSQL**: Certifique-se de que o PostgreSQL está instalado e em execução no seu sistema. Crie um banco de dados onde os dados serão armazenados.

2. **Instalar psycopg2**: Execute o seguinte comando para instalar a biblioteca psycopg2, que permite a conexão com o banco de dados PostgreSQL a partir do Python:

    ```bash
    pip install psycopg2
    ```

3. **Configurar as Credenciais do Banco de Dados**: No script Python, atualize a string de conexão com as credenciais corretas do seu banco de dados PostgreSQL:

    ```python
    conn = psycopg2.connect("host=localhost dbname=seu_db user=seu_usuario password=sua_senha")
    ```

## Execução

Para executar o script, navegue até o diretório onde o arquivo está localizado e execute-o com Python:

```bash
python nome_do_script.py
