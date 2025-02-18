"""
Autor: Vittor

Descrição: Este módulo define a classe VectorStoreBuilder que processa
um documento de texto para gerar embeddings e armazenar no PostgreSQL.

O objetivo é preparar os dados para usar RAG com os embeddings gerados 
a partir do livro "A Origem das Espécies".
"""
import os
import logging
import hashlib
from typing import List
from dotenv import load_dotenv
from langchain_core.documents import Document
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_openai import OpenAIEmbeddings
from langchain_community.vectorstores import PGVector
from langchain_community.document_loaders import TextLoader

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv(override=True)
# Define o formato dos logs
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,  # usado para para documentar e debugar
)
logger = logging.getLogger(__name__)


class VectorStoreBuilder:
    """Classe para gerar embeddings e armazenar no PostgreSQL.

    Essa classe implementa um pipeline que:
      1. Carrega um documento de texto a partir de um path.
      2. Divide o documento em chunks com valores recomendados para RAG de 
      de documentos científicos(1500 caracteres, com sobreposição de 20%).
      3. Gera embeddings para cada chunk utilizando o modelo "text-embedding-3-small" da OpenAI
        uma alternativa melhor seria otext-embedding-ada-002 com custo aproximado 5x maior
      4. Armazena os embeddings no PostgreSQL, marcando cada chunk com um hash do conteúdo 
      para controle de duplicidades quando a aplicação evoluir.
    """
    def __init__(self, content_path: str, collection_name: str):
        self.content_path = content_path
        self.collection_name = collection_name
        self.embedding_model = "text-embedding-3-small" #text-embedding-ada-002(custo 5x)
        self.config = {
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
            "host": os.getenv("DB_HOST"),
            "port": os.getenv("DB_PORT"),
            "database": os.getenv("DB_NAME"),
        }
        self.db_url = f"postgresql://{self.config['user']}:{self.config['password']}@{self.config['host']}:{self.config['port']}/{self.config['database']}"

    def _load_document(self) -> List[Document]:
        """Carrega o documento de texto do caminho especificado"""
        try:
            logger.info("Carregando o arquivo...")
            loader = TextLoader(self.content_path)
            return loader.load()
        except Exception as e:
            logger.error("Falha ao carregar documento: %s", {str(e)})
            raise

    def _split_text(self, documentos: List[Document]) -> List[Document]:
        """Gera os chunks de 1500, um valor recomendado para manter a coerência e contexto sem 
        sobrecarregar o modelo. Para a com sobreposição estou usando 20% que é 
        um valor seguro para evitar perder contexto na trasição de um chunk para outro."""

        logger.info("Gerandos os chunks...")
        splitter = RecursiveCharacterTextSplitter(
            chunk_size=1500, chunk_overlap=300, separators=["\n\n", "\n", " "]
        )
        return splitter.split_documents(documentos)

    def _build_embeddings(self, chunks: List[Document], content_hash: str) -> PGVector:
        """Gera e armazena embeddings marcando pre_delete_collection como true para garantir 
        que a coleção seja excluída antes de inserir novos embeddings e assim evitar duplicações.
        Também estou adicionando o hash do conteúdo como metadados para caso no futuro seja incluída 
        a possibilidade de atualizar os embeddings e assim possa ser feita facilmente a verificação 
        de duplicidade."""
        try:
            logger.info("Gerando embeddings...")
            embeddings = OpenAIEmbeddings(model=self.embedding_model)

            for chunk in chunks:
                chunk.metadata["content_hash"] = content_hash

            return PGVector.from_documents(
                documents=chunks,
                embedding=embeddings,
                collection_name=self.collection_name,
                connection_string=self.db_url,
                pre_delete_collection=True,
            )
        except Exception as e:
            logger.error("Erro ao gerar embeddings: %s", {str(e)})
            raise

    def _hash_document_content(self, documentos: List[Document]) -> str:
        logger.info("Gerando hash SHA-256 ")
        content = "".join([doc.page_content for doc in documentos])
        return hashlib.sha256(content.encode()).hexdigest()

    def run_pipeline(self) -> None:
        """Pipeline para gerar embeddings e armazenar no PostgreSQL."""
        logger.info("Iniciando pipeline para gerar as embeddings...")

        documentos = self._load_document()
        logger.info("Documento carregado %s páginas", len(documentos))

        content_hash = self._hash_document_content(documentos)
        logger.info("Hash do conteúdo gerado: %s", {content_hash})

        chunks = self._split_text(documentos)
        logger.info("Texto dividido em %s chunks", {len(chunks)})

        self._build_embeddings(chunks, content_hash)
        logger.info("Embeddings gerados/atualizados com sucesso no PostgreSQL")
