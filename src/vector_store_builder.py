"""
Autor: Vittor

Descrição: Este módulo define a classe VectorStoreBuilder que processa
um documento de texto para gerar embeddings e armazenar no PostgreSQL.

O objetivo é preparar os dados para usar RAG com os embeddings gerados
a partir do livro A Origem das Espécies
"""

import os
import logging
import uuid
import json
from typing import List, Tuple
import concurrent.futures

import psycopg2
from dotenv import load_dotenv
from langchain_core.documents import Document
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_openai import OpenAIEmbeddings
from langchain_community.document_loaders import TextLoader

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv(override=True)

# Configuração de logs
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


class VectorStoreBuilder:
    """Classe para gerar embeddings e armazenar no PostgreSQL"""

    def __init__(self, content_path: str):
        self.content_path = content_path
        self.embedding_model = "text-embedding-ada-002"
        self.config = {
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
            "host": os.getenv("DB_HOST"),
            "port": os.getenv("DB_PORT"),
            "database": os.getenv("DB_NAME"),
        }

        self.db_url = (
            f"dbname='{self.config['database']}' "
            f"user='{self.config['user']}' "
            f"password='{self.config['password']}' "
            f"host='{self.config['host']}' "
            f"port='{self.config['port']}'"
        )
        # Definição dos lotes de processamento
        self.api_batch_size = 50
        self.thread_workers = 4

    def _load_document(self) -> List[Document]:
        """Carrega o documento de texto a partir do caminho especificado."""
        try:
            logger.info("Carregando o arquivo...")
            loader = TextLoader(self.content_path)
            return loader.load()
        except Exception as e:
            logger.error("Falha ao carregar documento: %s", str(e))
            raise

    def _split_text(self, documentos: List[Document]) -> List[Document]:
        """Gera os chunks de 1500, um valor recomendado para manter a coerência e contexto sem
        sobrecarregar o modelo. Para a com sobreposição estou usando 20% que é
        um valor seguro para evitar perder contexto na trasição de um chunk para outro.
        """

        logger.info(
            "Gerando os chunks utilizando RecursiveCharacterTextSplitter para manter formatação..."
        )
        splitter = RecursiveCharacterTextSplitter(
            chunk_size=1500, chunk_overlap=300, separators=["\n\n", "\n", " "]
        )
        return splitter.split_documents(documentos)

    def _create_embeddings_batch(
        self, batch: List[Document], embeddings: OpenAIEmbeddings
    ) -> List[Tuple[str, str, str, str]]:
        """
        Processa um batch de documentos:
          - Gera embeddings para todos os textos do batch.
          - Prepara os dados para inserção no banco.
        Retorna uma lista de tuplas: (doc_id, pageContent, metadata, embedding_str)
        """
        texts = [doc.page_content for doc in batch]
        # Chama a API para gerar embeddings para o batch
        embedding_vectors = embeddings.embed_documents(texts)
        results = []
        for doc, vector in zip(batch, embedding_vectors):
            doc_id = str(uuid.uuid4())
            # Formata o vetor para o formato esperado pelo pgvector: [0.1,0.2,0.3,...]
            embedding_str = "[" + ",".join(map(str, vector)) + "]"
            results.append(
                (
                    doc_id,
                    doc.page_content,
                    json.dumps(doc.metadata),
                    embedding_str,
                )
            )
        return results

    def _persist_embbedings(self, chunks: List[Document]) -> None:
        """Gera embeddings em batches de forma concorrente e insere os registros na tabela 'documents'."""
        logger.info(
            "Iniciando a geração de embeddings e inserção na tabela 'documents'..."
        )

        embeddings = OpenAIEmbeddings(model=self.embedding_model)
        all_results = []

        # Processa os batches em paralelo
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.thread_workers
        ) as executor:
            futures = []
            for i in range(0, len(chunks), self.api_batch_size):
                batch = chunks[i : i + self.api_batch_size]
                futures.append(
                    executor.submit(self._create_embeddings_batch, batch, embeddings)
                )
            for future in concurrent.futures.as_completed(futures):
                try:
                    batch_results = future.result()
                    all_results.extend(batch_results)
                except Exception as e:
                    logger.error("Erro ao processar lote: %s", str(e))

        logger.info("Total de chuncks processados: %d", len(all_results))

        # Inserção em lote no banco de dados
        connection = psycopg2.connect(self.db_url)
        cursor = connection.cursor()
        insert_query = """
            INSERT INTO documents (id, "pageContent", metadata, embedding)
            VALUES (%s, %s, %s, %s)
        """
        # Usando o executemany para inserir os registros em lote
        cursor.executemany(insert_query, all_results)
        connection.commit()
        cursor.close()
        connection.close()
        logger.info("Embeding gerados e persistidos com sucesso na tabela 'documents'.")

    def run_pipeline(self) -> None:
        """Executa o pipeline completo para gerar e armazenar os embeddings."""
        logger.info("Iniciando pipeline para gerar as embeddings...")

        documentos = self._load_document()
        logger.info("Documento carregado. Quantidade de páginas: %d", len(documentos))

        chunks = self._split_text(documentos)
        logger.info("Texto dividido em %d chunks.", len(chunks))

        self._persist_embbedings(chunks)
        logger.info("Embeddings gerados e armazenados com sucesso no PostgreSQL.")
