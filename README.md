# Simulador de algorítmo de consenso RAFT

Projeto realizado em conjunto com Bruna Guerreiro e Pedro Lanzarini

O objetivo deste projeto é simular o funcionamento do algorítmo de consenso RAFT como parte avaliativa da disciplina de Sistemas Distribuídos pela Universidade Federal Fluminense (UFF)

O projeto foi desenvolvido com base no artigo de definição do sistema RAFT "In Search of an Understandable Consensus Algorithm" publicado por Ongaro e Ousterhout da universidade de Stanford: https://raft.github.io/raft.pdf
Como base para a implementação e testes foi utilizado o laboratório Lab2 disponível em http://www.cs.bu.edu/~jappavoo/jappavoo.github.com/451/labs/lab-raft.html do curso CS451/651 Distributed Systems.

## Execução

Para executar o projeto Raft é necessário informar ao GO qual o path do projeto para que ele consiga importar corretamente o LabRPC. Isso deve ser feito adicionando o LabRPC na src/ de instalação do GO ou executando os seguintes passos:
```
$ export GOPATH = <caminho do projeto>
$ cd $GOPATH
$ cd src/raft
$ go test –run 2A
```

Caso não adicione o LabRPC na src/ de instalação do seu GO é importante realizar
o passo de adicionar o GOPATH toda vez que abrir uma nova instância de terminal para
executar o programa. Outra opção é adicionar permanentemente no arquivo do seu
programa de terminal (Bash por exemplo).