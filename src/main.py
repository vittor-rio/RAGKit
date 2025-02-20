from pathlib import Path
from vector_store_builder import VectorStoreBuilder


def main():
    """ "Executa pipile para gerar embeddings do livro"""
    # Define o caminho base para encontrar o arquivo de dados
    the_origin_of_species_book = (
        Path(__file__).parent.parent / "data/theOriginOfSpecies.txt"
    )

    # Instancia e executa o pipeline de embeddings
    pipeline = VectorStoreBuilder(
        content_path=str(the_origin_of_species_book),
    )
    pipeline.run_pipeline()


if __name__ == "__main__":
    main()
