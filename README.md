# RAGKit :: Flowise ChatFlow + Postgres Vector DBs

o Flowise para orquestração e o Postgres com a extensão pgvector para armazenamento dos vetores de embedding. O conteúdo utilizado nos vetores pode ser definido de acordo com o seu projeto, como exemplo, no código estará utilizando o livro _A Origem das Espécies_.

![Descrição da Arquitetura](docs/architecture.png)

## Configure o Flowise

- [Guia de Instalação](https://docs.flowiseai.com/getting-started): Instruções para configurar o Flowise em sua máquina local.

- [Fazer a configuração do Postgres(ou equivalente)](https://docs.flowiseai.com/configuration/databases): Orientações sobre como conectar sua instância do Flowise a diferentes tipos de bancos de dados.

Para garantir a segurança ao expor o Flowise publicamente, é essencial habilitar a autenticação. A documentação oficial do Flowise fornece orientações detalhadas sobre como configurar a autenticação em nível de aplicativo e de chatflow.

### Exemplo de Configuração de um ChatFlow no Flowise

O arquivo [`flowise/Case O Boticário __ GenAI __ RAG Chatflow.json`](flowise/Case%20O%20Botic%C3%A1rio%20__%20GenAI%20__%20RAG%20Chatflow.json) contém um exemplo de configuração de um chat que implementa a técnica de **RAG** utilizando uma base de vetores no PostgreSQL. Este exemplo demonstra como integrar o Flowise com um banco de dados vetorial para aprimorar as respostas do chat.

Para garantir a segurança ao expor o Flowise publicamente, é essencial habilitar a autenticação. A documentação oficial do Flowise fornece orientações detalhadas sobre como configurar a autenticação em nível de aplicativo e de chatflow.
