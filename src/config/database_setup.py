"""
Autor: Vittor

Descrição: Neste modulo temos a configuração do Banco de Dados para receber os Vector DBs. 

Bascicamente prepara o ambiente do PostgreSQL para armazenar os embeddings,
utilizando uma estratégia centralizada onde teremos um único banco para diversos
conteúdos que possam virar embeddings.
"""

import os
import logging
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv(override=True)
# Define o formato dos logs
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,  # usado para para documentar e debugar
)
logger = logging.getLogger("database_setup")

class DatabaseSetup:
    """ Configura o postgres para armazenar os embeddings"""
    def __init__(self):
        logger.info("Inicializando as configurações de conexão com o banco...")
        self.config = {
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
            "host": os.getenv("DB_HOST"),
            "port": os.getenv("DB_PORT"),
            "database": os.getenv("DB_DEFAULT_NAME"),  # Usar o DB padrão para conexão inicial
        }

        self.rag_db = os.getenv("DB_NAME")  # Nome do banco para os embeddings

    def _connect(self, autocommit: bool = False):
        logger.info("Criando conexão com o postgres...")
        try:
            connection = psycopg2.connect(**self.config)
            connection.autocommit = autocommit
            return connection
        except Exception as error:
            logger.error("Erro ao conectar ao banco de dados: %s", error)
            raise

    def create_database(self):
        """Cria ou valida se existe o banco de dados 'rag_db'"""
        try:
            logger.info("Criando/validando o DB vetorial 'rag_db...'")
            connection = self._connect(autocommit=True)
            cursor = connection.cursor()

            # Verifica se o banco já existe
            cursor.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s", (self.rag_db,)
            )
            if not cursor.fetchone():
                cursor.execute(
                    sql.SQL("CREATE DATABASE {}").format(sql.Identifier(self.rag_db))
                )
                logger.info("Banco de dados '%s' criado com sucesso", self.rag_db)
            else:
                logger.info("Banco de dados '%s' já existe! OK", self.rag_db)

        except Exception as error:
            logger.error("Falha na criação/validação do banco: %s", error)
            raise
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

def main():

    try:
        configurator = DatabaseSetup()

        logger.info("Criando/Validando o banco de dados...")
        configurator.create_database()
        logger.info("Configuração concluída com sucesso! " )

    except Exception as error:
        logger.error("Ocorreu ao configurar o banco de dados: %s", error)
        exit(1)

if __name__ == "__main__":
    main()
