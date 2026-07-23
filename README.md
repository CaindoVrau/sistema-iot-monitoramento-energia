# Sistema IoT de Monitoramento e Controle de Energia Elétrica

Projeto desenvolvido como Trabalho de Conclusão de Curso do curso de Engenharia de Controle e Automação do Instituto Federal de São Paulo – Campus Hortolândia.

O sistema realiza o monitoramento de grandezas elétricas utilizando ESP32 e PZEM-004T, com transmissão dos dados por MQTT, armazenamento em SQLite e visualização por meio de um dashboard desenvolvido em Streamlit.

## Tecnologias utilizadas

- ESP32 DevKit V1
- PZEM-004T V4.0 100 A
- MQTT e Mosquitto
- Python
- Paho MQTT
- SQLite
- Streamlit
- Tailscale

## Estrutura do repositório

```text
sistema-iot-monitoramento-energia/
├── firmware/
│   └── esp32_monitoramento.ino
├── ingestor/
│   └── ingestor.py
├── dashboard/
│   └── app.py
├── config/
│   └── config.example.json
├── docs/
│   └── esquematico-eletrico.pdf
├── requirements.txt
├── .gitignore
└── README.md
```

## Requisitos

Para executar a aplicação no computador servidor, são necessários:

- Python 3;
- broker MQTT Mosquitto;
- dependências listadas no arquivo `requirements.txt`;
- ESP32 previamente gravado com o firmware do projeto.

## Instalação das dependências Python

Na pasta principal do projeto, execute:

```bash
pip install -r requirements.txt
```

Recomenda-se utilizar um ambiente virtual Python.

No Windows:

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## Configuração do sistema

Antes da execução, verifique as configurações utilizadas pelo ingestor e pelo dashboard, especialmente:

- endereço do broker MQTT;
- porta do broker;
- usuário e senha, quando aplicáveis;
- caminho do banco de dados SQLite.

Credenciais reais não devem ser publicadas no repositório. Utilize o arquivo de exemplo disponível na pasta `config`.

## Execução do ingestor

Com o broker Mosquitto em execução, inicie o serviço de ingestão:

```bash
python ingestor/ingestor.py
```

O ingestor permanece conectado ao broker, recebe as mensagens MQTT, valida os payloads e registra as informações no banco SQLite.

## Execução do dashboard

Em outro terminal, execute:

```bash
streamlit run dashboard/app.py
```

Após a inicialização, o Streamlit informará o endereço local para acesso ao dashboard.

## Provisionamento do ESP32

O provisionamento é realizado por comunicação USB/serial, na velocidade de 115200 bit/s.

O aplicativo envia uma linha iniciada por `CFG:`, seguida por um objeto JSON:

```text
CFG:{"wifi":{"ssid":"NOME_DA_REDE","psk":"SENHA_DA_REDE"},"mqtt":{"host":"IP_DO_SERVIDOR","port":1883,"user":"USUARIO","pass":"SENHA"},"device":{"lugar":"casa","ambiente":"quarto","medicao":"energia","dispositivo":"esp-001"},"telemetry_period_ms":5000}
```

Após receber uma configuração válida, o ESP32:

1. interpreta o JSON;
2. armazena os parâmetros na memória não volátil;
3. responde `ACK:OK` pela serial;
4. conecta-se novamente ao Wi-Fi e ao broker MQTT.

O provisionamento não grava o firmware. O código principal deve estar previamente instalado no ESP32.

## Estrutura dos tópicos MQTT

Os tópicos seguem o padrão:

```text
{lugar}/{ambiente}/{medicao}/{dispositivo}/{fluxo}
```

Exemplo:

```text
casa/quarto/energia/esp-001/medicao
casa/quarto/energia/esp-001/status
casa/quarto/energia/esp-001/cmd
casa/quarto/energia/esp-001/ack
```

| Fluxo | Finalidade |
|---|---|
| `medicao` | Publicação das grandezas elétricas |
| `status` | Estado online ou offline do dispositivo |
| `cmd` | Comandos enviados ao ESP32 |
| `ack` | Confirmação do processamento dos comandos |

## Payload de telemetria

Exemplo de mensagem publicada pelo ESP32:

```json
{
  "seq": 125,
  "p": 84.7,
  "vrms": 127.2,
  "irms": 0.81,
  "pf": 0.82,
  "e": 1.458,
  "f": 60.0
}
```

| Campo | Descrição | Unidade |
|---|---|---|
| `seq` | Número sequencial da mensagem | — |
| `p` | Potência ativa | W |
| `vrms` | Tensão eficaz | V |
| `irms` | Corrente eficaz | A |
| `pf` | Fator de potência | — |
| `e` | Energia acumulada | kWh |
| `f` | Frequência da rede | Hz |

## Comando do relé

Exemplo de comando para acionamento do primeiro canal:

```json
{
  "type": "relay",
  "channel": 1,
  "state": "ON",
  "req_id": "cmd-001"
}
```

Exemplo de confirmação enviada pelo ESP32:

```json
{
  "req_id": "cmd-001",
  "ok": true,
  "details": "Relay 1 -> ON"
}
```

## Pinagem do ESP32

| Função | GPIO |
|---|---:|
| Recepção UART do PZEM | GPIO16 – RX2 |
| Transmissão UART para o PZEM | GPIO17 – TX2 |
| Controle do módulo de relé | GPIO26 |
| LED de estado do Wi-Fi | GPIO22 |
| LED de estado do MQTT | GPIO23 |

A comunicação UART deve ser cruzada:

```text
ESP32 TX2 / GPIO17 → PZEM RX
PZEM TX → ESP32 RX2 / GPIO16
```

## Autor

Caio Traldi Sant'Ana  
Engenharia de Controle e Automação  
Instituto Federal de São Paulo – Campus Hortolândia
