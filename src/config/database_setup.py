"""
Autor: Vittor

Descrição: Configuração do Banco de Dados para receber os Vector DBs do livro. Bascicamente prepara o ambiente do PostgreSQL para armazenar os embeddings,
utilizando uma estratégia centralizada onde teremos um único banco para diversos conteúdos que possam virar embeddings.
"""

import os
import logging
import psycopg2
from psycopg2.errors import DuplicateDatabase
from psycopg2 import sql
from dotenv import load_dotenv

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv(override=True)

# Configura o sistema de logs
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO 
)

#O logger.info será usado como uma forma de documentar o codigo e ao mesmo tempo será útil para debugar 
logger = logging.getLogger('database_setup')

class DatabaseSetup:
    
    def __init__(self):
        logger.info("Inicializando as configurações de conexão com o banco.")
        self.config = {
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'host': os.getenv('DB_HOST'),
            'port': os.getenv('DB_PORT'),
            'database': 'postgres'  # Usar o padrão para conexão inicial
        }

        self.target_db = os.getenv('DB_NAME', 'rag_db')  # Nome do banco para os embeddings
    
    def _connect(self, autocommit: bool = False):
        logger.info("Criando conexão com o postgres")
        try:
            connection = psycopg2.connect(**self.config)
            connection.autocommit = autocommit 
            return connection
        except Exception as error:
            logger.error("Erro ao conectar ao banco de dados: %s", error)
            raise
    
    def create_database(self):
        logger.info("Criando/validando o DB vetorial 'rag_db'")
        connection = None
        try:
            connection = self._connect(autocommit=True)  # Conexão fora do bloco with
            cursor = connection.cursor()
            
            # Verifica se o banco já existe
            cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (self.target_db,))
            if not cursor.fetchone():
                cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(self.target_db)))
                logger.info(f"Banco de dados '{self.target_db}' criado com sucesso")
            else:
                logger.info(f"Banco de dados '{self.target_db}' já existe! OK")
                
        except Exception as error:
            logger.error("Falha na criação do banco: %s", error)
            raise
        finally:
            if cursor: cursor.close()
            if connection: connection.close() 
    
    def setup_structure(self):
        logger.info("Preparando o db para armazenar as embeddings")
        try:
            self.config['database'] = 'rag_db'
            
            with self._connect() as connection:
                with connection.cursor() as cursor:
                    cursor.execute("CREATE EXTENSION IF NOT EXISTS vector")
                    logger.info("Extensão 'vector' criada/validada")
                    
                    logger.info("Criando/validando a tabela para armazenar os embedings")
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS documents (
                            id SERIAL PRIMARY KEY,
                            content TEXT NOT NULL,
                            vector VECTOR(1536),
                            created_at TIMESTAMP DEFAULT NOW()
                        )
                    """)
                    logger.info("Tabela 'documents' criada/validada com sucesso")
                    
                    logger.info("Cria/valida índice para performar nas buscas")
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS vector_index
                        ON documents USING ivfflat (vector vector_cosine_ops)
                    """)
                    logger.info("Índice 'vector_index' criado/verificado")
                    
                    connection.commit()
                    logger.info("Estrutura do DB configurada com sucesso")
        except Exception as error:
            logger.error("Erro na preparação da estrutura do banco: %s", error)
            raise

def main():
    logger.info("Iniciando configuração/validação do banco de dados PostgreSQL...")
    
    try:
        configurator = DatabaseSetup()
        
        logger.info("Passo 1 - Criando banco de dados...")
        configurator.create_database()
        
        logger.info("Passo 2 - Preparando tabelas e índices...")
        configurator.setup_structure()
        
        logger.info("Configuração concluída com sucesso! O banco está pronto para receber os dados do livro.")
    
    except Exception as error:
        logger.error("Ocorreu um problema grave: %s", error)
        exit(1)

if __name__ == "__main__":
    main()