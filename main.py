import psycopg2
import csv

# Estabeleça a conexão com o banco de dados
conn = psycopg2.connect("host=localhost dbname=ADA user=postgres password=123")
cur = conn.cursor()

# Desative o commit automático
conn.autocommit = False

try:
    # Criação das tabelas
    cur.execute("""
        CREATE TABLE IF NOT EXISTS regioes (
            id SERIAL PRIMARY KEY,
            noc VARCHAR(3),
            regiao VARCHAR(255),
            notes VARCHAR(255),
            CONSTRAINT unico_noc_region UNIQUE (noc, regiao)
        );
    """)

    # Inserção dos dados de regiões
    with open('noc_regions.csv', 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)  # Pula o cabeçalho
        for row in reader:
            cur.execute("""
                INSERT INTO regioes (noc, regiao, notes)
                VALUES (%s, %s, %s)
                ON CONFLICT ON CONSTRAINT unico_noc_region DO NOTHING;
            """, (row[0], row[1], row[2]))

    # Criação das outras tabelas
    cur.execute("""
        CREATE TABLE IF NOT EXISTS times (
            id SERIAL PRIMARY KEY,
            nome VARCHAR(50),
            noc VARCHAR(50),
            CONSTRAINT unico_team_noc UNIQUE (nome, noc)
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS atletas (
            id SERIAL PRIMARY KEY,
            nome VARCHAR(100),
            genero CHAR(1),
            idade INTEGER,
            altura NUMERIC,
            peso NUMERIC,
            noc VARCHAR(3),
            time_id INTEGER REFERENCES times(id),
            CONSTRAINT unico_nome_noc UNIQUE (nome, noc)
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS esportes (
            id SERIAL PRIMARY KEY,
            nome VARCHAR(50),
            temporada VARCHAR(6),
            regiao_id INTEGER REFERENCES regioes(id),
            CONSTRAINT unico_nome_temporada_regiao_id UNIQUE (nome, temporada, regiao_id)
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS modalidades (
            id SERIAL PRIMARY KEY,
            nome VARCHAR(100),
            esportes_id INTEGER REFERENCES esportes(id),
            CONSTRAINT unico_nome_esportes_id UNIQUE (nome, esportes_id)
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS medalhas (
            id SERIAL PRIMARY KEY,
            atletas_id INTEGER REFERENCES atletas(id),
            modalidades_id INTEGER REFERENCES modalidades(id),
            medalha VARCHAR(10),
            ano INTEGER,
            CONSTRAINT unico_atletas_modalidades UNIQUE (atletas_id, modalidades_id)
        );
    """)

    # Dicionário para armazenar IDs de times para evitar consultas repetidas
    times_ids = {}

    # Inserção dos dados de atletas e relacionados
    with open('athlete_events.csv', 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            if 'NA' not in [row[3], row[4], row[5]]:
                idade = int(row[3]) if row[3] != 'NA' else None

                # Otimização para inserção em times
                time_key = (row[6], row[8])
                if time_key not in times_ids:
                    cur.execute("""
                        INSERT INTO times (nome, noc)
                        VALUES (%s, %s)
                        ON CONFLICT ON CONSTRAINT unico_team_noc DO NOTHING
                        RETURNING id;
                    """, (row[6], row[8]))
                    times_id = cur.fetchone()[0]
                    times_ids[time_key] = times_id
                else:
                    times_id = times_ids[time_key]

                # Inserções para atletas, esportes, modalidades, medalhas
                cur.execute("""
                    INSERT INTO atletas (nome, genero, idade, altura, peso, noc, time_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT ON CONSTRAINT unico_nome_noc DO NOTHING;
                """, (row[1], row[2], idade, row[4], row[5], row[7], times_id))

                cur.execute("""
                    INSERT INTO esportes (nome, temporada, regiao_id)
                    VALUES (%s, %s, (SELECT id FROM regioes WHERE noc = %s LIMIT 1))
                    ON CONFLICT ON CONSTRAINT unico_nome_temporada_regiao_id DO NOTHING;
                """, (row[12], row[10], row[7]))

                cur.execute("""
                    INSERT INTO modalidades (nome, esportes_id)
                    VALUES (%s, (SELECT id FROM esportes WHERE nome = %s AND temporada = %s LIMIT 1))
                    ON CONFLICT ON CONSTRAINT unico_nome_esportes_id DO NOTHING;
                """, (row[13], row[12], row[10]))

                cur.execute("""
                    INSERT INTO medalhas (atletas_id, modalidades_id, medalha, ano)
                    VALUES (
                        (SELECT id FROM atletas WHERE nome = %s AND noc = %s LIMIT 1),
                        (SELECT id FROM modalidades WHERE nome = %s AND esportes_id = (SELECT id FROM esportes WHERE nome = %s AND temporada = %s LIMIT 1) LIMIT 1),
                        %s, %s
                    ) ON CONFLICT ON CONSTRAINT unico_atletas_modalidades DO NOTHING;
                """, (row[1], row[7], row[13], row[12], row[10], row[14], row[9]))

    # Commit das operações
    conn.commit()

except Exception as e:
    print("Erro:", e)
    conn.rollback()

finally:
    cur.close()
    conn.close()
