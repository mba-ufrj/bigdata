## Crime analysis.

**Grupo**
- Arthur dos Reis
- Diogo Albuquerque
- Felipe Veloso
- Marcio Grey
- Marcos Sant'Anna
- Vitor Sampaio

Com o objetivo de diminuir as ocorrências e tornar os locais menos atrativos para ações criminosas, essa analise irá gerar insights para que possam ser mapeadas constantemente o perfil das vítimas, os locais dos acontecimentos e as tendências de acontecimentos.

Permitindo assim que as forças de proteção direcionem os seus esforços e olhares para esses perfis e locais.

O programa tem como objetivo ser executado **mensalmente**, gerando assim dados para que possam ser feitas as análises.

### Carregamento das bibliotecas utilizadas
 
Para estruturar o programa, fazemos o load das bibliotecas que iremos utilizar.


```scala
// Import da biblioteca do sql.

import org.apache.spark.sql
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

// Import da biblioteca do date.

import java.time.LocalDate
```


    VBox()


    Starting Spark application



<table>
<tr><th>ID</th><th>YARN Application ID</th><th>Kind</th><th>State</th><th>Spark UI</th><th>Driver log</th><th>Current session?</th></tr><tr><td>0</td><td>application_1602524684428_0001</td><td>spark</td><td>idle</td><td><a target="_blank" href="http://ip-172-31-4-173.ec2.internal:20888/proxy/application_1602524684428_0001/">Link</a></td><td><a target="_blank" href="http://ip-172-31-1-146.ec2.internal:8042/node/containerlogs/container_1602524684428_0001_01_000001/livy">Link</a></td><td>✔</td></tr></table>



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    SparkSession available as 'spark'.



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    import org.apache.spark.sql
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.SaveMode
    import org.apache.spark.sql.functions._
    import java.time.LocalDate


### Carregar o dataset

Para o trabalho, nós escolhemos um dataset que contem as ocorrências de crimes.


```scala
// Carregando o dataset de ocorrências.
val crimeRDO = spark.read.option("header", "true").csv("s3://study-bi/rdo/rdo.csv")
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    crimeRDO: org.apache.spark.sql.DataFrame = [ID_DELEGACIA: string, NOME_DEPARTAMENTO: string ... 29 more fields]



```scala
// Apresentando o schema dos dados.
crimeRDO.printSchema()
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    root
     |-- ID_DELEGACIA: string (nullable = true)
     |-- NOME_DEPARTAMENTO: string (nullable = true)
     |-- NOME_SECCIONAL: string (nullable = true)
     |-- NOME_DELEGACIA: string (nullable = true)
     |-- CIDADE: string (nullable = true)
     |-- ANO_BO: string (nullable = true)
     |-- NUM_BO: string (nullable = true)
     |-- NOME_DEPARTAMENTO_CIRC: string (nullable = true)
     |-- NOME_SECCIONAL_CIRC: string (nullable = true)
     |-- NOME_DELEGACIA_CIRC: string (nullable = true)
     |-- NOME_MUNICIPIO_CIRC: string (nullable = true)
     |-- DESCR_TIPO_BO: string (nullable = true)
     |-- DATA_OCORRENCIA_BO: string (nullable = true)
     |-- HORA_OCORRENCIA_BO: string (nullable = true)
     |-- DATAHORA_COMUNICACAO_BO: string (nullable = true)
     |-- FLAG_STATUS: string (nullable = true)
     |-- RUBRICA: string (nullable = true)
     |-- DESCR_CONDUTA: string (nullable = true)
     |-- DESDOBRAMENTO: string (nullable = true)
     |-- DESCR_TIPOLOCAL: string (nullable = true)
     |-- DESCR_SUBTIPOLOCAL: string (nullable = true)
     |-- LOGRADOURO: string (nullable = true)
     |-- NUMERO_LOGRADOURO: string (nullable = true)
     |-- LATITUDE: string (nullable = true)
     |-- LONGITUDE: string (nullable = true)
     |-- DESCR_TIPO_PESSOA: string (nullable = true)
     |-- FLAG_VITIMA_FATAL: string (nullable = true)
     |-- SEXO_PESSOA: string (nullable = true)
     |-- IDADE_PESSOA: string (nullable = true)
     |-- COR_CUTIS: string (nullable = true)
     |-- _c30: string (nullable = true)
    



```scala
// Apresentando a quantidade de registros utilizados.
crimeRDO.count()
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    res10: Long = 553429


### Definição de variáveis

Definindo as variáveis que serão utilizadas na execução recorrente, o ano da consulta e o mes da consulta.

**Para exemplo vamos utilizar o ano de 2016.**


```scala
val yearSelect = LocalDate.now.getYear - 4 // Utilizando o ano atual - 4 anos = 2016
val monthSelect = LocalDate.now.getMonthValue - 1 // Analisando o mês anterior.
val storagePath = yearSelect + "/" + monthSelect + "/" // path onde será armazenado os dados
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    yearSelect: Int = 2016
    monthSelect: Int = 9
    storagePath: String = 2016/9/


### Análise de tendência de crimes

Com essa análise será possível ver a tendência dos crimes nos últimos **6 meses**, 
dessa forma, será possível observar a curva de ocorrências.


```scala
// Analise da tendencia da quantidade de crimes, se esta diminuindo ou nao, analisado os ultimos 6 meses
//e armazenado o arquivo para apresentação.
val crimeTrend = crimeRDO.filter(col("ANO_BO") === yearSelect &&
                                 month(to_date(col("DATA_OCORRENCIA_BO"), "dd/MM/yyyy")).
                                 lt(monthSelect + 1) &&
                                 month(to_date(col("DATA_OCORRENCIA_BO"), "dd/MM/yyyy")).
                                 gt(monthSelect - 6)).
withColumn("MES", month(to_date(col("DATA_OCORRENCIA_BO"), "dd/MM/yyyy"))).
groupBy("MES").
agg(count("NUM_BO").as("OCORRENCIAS")).
sort(asc("MES")).select("MES", "OCORRENCIAS")

crimeTrend.write.mode(SaveMode.Overwrite).
json("s3://study-bi/rdo/" + storagePath + "trend/")
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    crimeTrend: org.apache.spark.sql.DataFrame = [MES: int, OCORRENCIAS: bigint]



```scala
// Apresentando a quantidade de registros do dataframe de tendencias.
crimeTrend.count()
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    res26: Long = 6



```scala
// Apresentando os dados do dataframe de tendencias.

crimeTrend.show()
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    +---+-----------+
    |MES|OCORRENCIAS|
    +---+-----------+
    |  4|      42734|
    |  5|      42493|
    |  6|      41149|
    |  7|      42625|
    |  8|      43390|
    |  9|      42919|
    +---+-----------+
    


### Filtrando o DataFrame

Para que as análises possam ser executadas, iremos pegar do dataframe completo apenas os registros para o **mês** 
e o **ano** de execução do programa.

Executamos a persistência desse dataframe, pois ele será parte importante de futuras análises.


```scala
// Filtrando o RDO original e persistindo ele para futuras consultas.
val yearMonthRDO = crimeRDO.filter(col("ANO_BO") === yearSelect &&
                                   month(to_date(col("DATA_OCORRENCIA_BO"), "dd/MM/yyyy")) === monthSelect).
persist()
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    yearMonthRDO: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [ID_DELEGACIA: string, NOME_DEPARTAMENTO: string ... 29 more fields]



```scala
// Apresentando a quantidade de registros do dataframe filtrado por ano e mes.
yearMonthRDO.count()
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    res32: Long = 42919



```scala
// Apresentando os dados do dataframe filtrado por ano e mes.
yearMonthRDO.show(1, false)
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    +------------+----------------------------------------+----------------------------------------+------------------------------+----------------------------------------+------+------+----------------------------------------+----------------------------------------+------------------------------+------------------------------+----------------------------------------+------------------+------------------+-----------------------+-----------+-------------------------+-------------+-------------+----------------------------------------+--------------------------------------------------------------------------------+----------------------------------------+-----------------+------------+------------+--------------------+-----------------+-----------+------------+--------------------+----+
    |ID_DELEGACIA|NOME_DEPARTAMENTO                       |NOME_SECCIONAL                          |NOME_DELEGACIA                |CIDADE                                  |ANO_BO|NUM_BO|NOME_DEPARTAMENTO_CIRC                  |NOME_SECCIONAL_CIRC                     |NOME_DELEGACIA_CIRC           |NOME_MUNICIPIO_CIRC           |DESCR_TIPO_BO                           |DATA_OCORRENCIA_BO|HORA_OCORRENCIA_BO|DATAHORA_COMUNICACAO_BO|FLAG_STATUS|RUBRICA                  |DESCR_CONDUTA|DESDOBRAMENTO|DESCR_TIPOLOCAL                         |DESCR_SUBTIPOLOCAL                                                              |LOGRADOURO                              |NUMERO_LOGRADOURO|LATITUDE    |LONGITUDE   |DESCR_TIPO_PESSOA   |FLAG_VITIMA_FATAL|SEXO_PESSOA|IDADE_PESSOA|COR_CUTIS           |_c30|
    +------------+----------------------------------------+----------------------------------------+------------------------------+----------------------------------------+------+------+----------------------------------------+----------------------------------------+------------------------------+------------------------------+----------------------------------------+------------------+------------------+-----------------------+-----------+-------------------------+-------------+-------------+----------------------------------------+--------------------------------------------------------------------------------+----------------------------------------+-----------------+------------+------------+--------------------+-----------------+-----------+------------+--------------------+----+
    |10004       |DIRD - DEPTO IDENT.REG.DIV              |DIV.POL.PORTO/AERO/PROT.TURIS-DECADE    |06º D.P. METROPOLITANO        |S.PAULO                                 |2016  |1603  |DECAP                                   |DEL.SEC.2º SUL                          |16º D.P. VILA CLEMENTINO      |S.PAULO                       |Boletim de Ocorrência                   |01/09/2016        |11:15             |NULL                   |Consumado  |Lesão corporal (art. 129)|NULL         |NULL         |Terminal/Estação                        |Metrov. e ferroviário metrop.-Desembarque                                       |ESTAÇÃO METRO PRAÇA DA ARVORE           |0                |-23.61017385|-46.63788604|Vítima              |NULL             |I          |51          |Branca              |null|
    +------------+----------------------------------------+----------------------------------------+------------------------------+----------------------------------------+------+------+----------------------------------------+----------------------------------------+------------------------------+------------------------------+----------------------------------------+------------------+------------------+-----------------------+-----------+-------------------------+-------------+-------------+----------------------------------------+--------------------------------------------------------------------------------+----------------------------------------+-----------------+------------+------------+--------------------+-----------------+-----------+------------+--------------------+----+
    only showing top 1 row
    


### Execução por estado

Como a nossa ideia é a execução de uma base de registros de ocorrências online, iremos executar as análises por estado,
dessa forma vamos separar por estado.

**Em nosso datasource contém apenas o estado de São Paulo.**


```scala
// Pegando todos os estados que serão utilziados para consulta, no nosso caso somente, Sao Paulo.
val state = yearMonthRDO.select(trim(col("NOME_MUNICIPIO_CIRC"))).dropDuplicates()
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    state: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [trim(NOME_MUNICIPIO_CIRC): string]



```scala
// Apresentando o resultado do filtro por estado.
state.show()
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    +-------------------------+
    |trim(NOME_MUNICIPIO_CIRC)|
    +-------------------------+
    |                  S.PAULO|
    +-------------------------+
    


### Análise por municipio e perfil de vítimas

Para cada estado encontrado, será feita uma filtragem e armazenamento de dados para que possa ter 
insumos para as seguintes análises:

- Quantidade de ocorrências por cidade;
- Quantidade de ocorrências por sexo da vítima;
- Quantidade de ocorrências pelo range da idade da vítima (dados entre 8 e 120 anos para maior assertividade);
- Quantidade de ocorrências pela cor da pele da vítima;
- Quantidade de ocorrências pela localização do registro.


```scala
// Convertando para lista para poder executar operacoes internas.
// Para cada estado sera feita uma analise.
    state.collectAsList().forEach(st => {
        // Nome do estado
      val stateName = st.get(0)
      
        // Feita a analise de quantidade de ocorencias por cidade do estado e armazenado o arquivo para apresentação.
      val monthCityOccurence = yearMonthRDO.filter(trim(col("NOME_MUNICIPIO_CIRC")) === stateName).
        groupBy("CIDADE").agg(count("NUM_BO").as("OCORRENCIAS")).
        select(trim(col("CIDADE")).as("CIDADE"), col("OCORRENCIAS")).
        sort(desc("OCORRENCIAS"))
        
        monthCityOccurence.count()
        monthCityOccurence.show(false)

      monthCityOccurence.write.mode(SaveMode.Overwrite).
        json("s3://study-bi/rdo/" + storagePath + stateName + "/city/")

      //       Inicio da analise do perfil das vitimas baseado no mes.
      //       Analise do sexo das vitimas e armazenado o arquivo para apresentação.
      val monthSexVictimProfile = yearMonthRDO.filter(trim(col("NOME_MUNICIPIO_CIRC")) === stateName)
        .select(col("SEXO_PESSOA").as("SEXO")).
        groupBy(col("SEXO")).count().withColumnRenamed("count", "QUANTIDADE")

        monthSexVictimProfile.count()
        monthSexVictimProfile.show(false)
        
      monthSexVictimProfile.write.mode(SaveMode.Overwrite).
        json("s3://study-bi/rdo/" + storagePath + stateName + "/profile/sex/")

      //       Analise da idade das vitimas, limpando as incoerrencias, removendo anos menores que 8 e
      //      maiores que 120, por erros de digitacao e armazenado o arquivo para apresentação.
      val monthAgeVictimProfile = yearMonthRDO.filter(trim(col("NOME_MUNICIPIO_CIRC")) === stateName)
        .select(col("IDADE_PESSOA").as("IDADE")).
        filter(col("IDADE").gt(8) && col("IDADE").lt(120)).
        groupBy(col("IDADE")).count().withColumnRenamed("count", "QUANTIDADE").
        select((col("IDADE") / 10).cast(sql.types.IntegerType).as("IDADE"), col("QUANTIDADE")).
        groupBy(col("IDADE")).agg(sum(col("QUANTIDADE")).as("QUANTIDADE")).
        withColumn("IDADE_STRING",
          concat(col("IDADE"), lit("0"), lit("-"), col("IDADE"), lit("9"))).
        select(col("IDADE_STRING").as("IDADE"), col("QUANTIDADE")).
        sort(desc("QUANTIDADE"))

        
        monthAgeVictimProfile.count()
        monthAgeVictimProfile.show(false)

      monthAgeVictimProfile.write.mode(SaveMode.Overwrite).
        json("s3://study-bi/rdo/" + storagePath + stateName + "/profile/age/")

      //       Analise da cor da pele das vitimas e armazenado o arquivo para apresentação.
      val monthSkinVictimProfile = yearMonthRDO.filter(trim(col("NOME_MUNICIPIO_CIRC")) === stateName)
        .select(col("COR_CUTIS").as("COR")).
        groupBy(col("COR")).count().withColumnRenamed("count", "QUANTIDADE")

        monthSkinVictimProfile.count()
        monthSkinVictimProfile.show(false)
        
      monthSkinVictimProfile.write.mode(SaveMode.Overwrite).
        json("s3://study-bi/rdo/" + storagePath + stateName + "/profile/skin/")


      //       Analise da cor da pele das vitimas e armazenado o arquivo para apresentação.
      val locationOccurence = yearMonthRDO.filter(trim(col("NOME_MUNICIPIO_CIRC")) === stateName)
        .select("LATITUDE", "LONGITUDE").
        withColumn("LATITUDE", substring(col("LATITUDE"), 0, 8)).
        withColumn("LONGITUDE", substring(col("LONGITUDE"), 0, 8)).
        filter(!(col("LATITUDE") === "NULL")).filter(!(col("LONGITUDE") === "NULL")).
        filter(!(col("LATITUDE") === "Informaç")).filter(!(col("LONGITUDE") === "Informaç")).
        groupBy("LATITUDE", "LONGITUDE").
        agg(count("LATITUDE").as("OCORRENCIAS")).
        sort(desc("OCORRENCIAS"))
        
        locationOccurence.count()
        locationOccurence.show(false)

      locationOccurence.write.mode(SaveMode.Overwrite).
        json("s3://study-bi/rdo/" + storagePath + stateName + "/location/")

    })
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    +---------------------+-----------+
    |CIDADE               |OCORRENCIAS|
    +---------------------+-----------+
    |S.PAULO              |42882      |
    |GUARULHOS            |9          |
    |DIADEMA              |9          |
    |FERRAZ DE VASCONCELOS|6          |
    |ITAPECERICA DA SERRA |4          |
    |OSASCO               |3          |
    |MAUA                 |2          |
    |S.ANDRE              |1          |
    |TABOAO DA SERRA      |1          |
    |CAIEIRAS             |1          |
    |S.BERNARDO DO CAMPO  |1          |
    +---------------------+-----------+
    
    +----+----------+
    |SEXO|QUANTIDADE|
    +----+----------+
    |M   |23099     |
    |F   |19749     |
    |I   |71        |
    +----+----------+
    
    +-------+----------+
    |IDADE  |QUANTIDADE|
    +-------+----------+
    |20-29  |12431     |
    |30-39  |11375     |
    |40-49  |7570      |
    |50-59  |4481      |
    |10-19  |3585      |
    |60-69  |2095      |
    |70-79  |700       |
    |80-89  |168       |
    |00-09  |28        |
    |90-99  |16        |
    |110-119|4         |
    |100-109|1         |
    +-------+----------+
    
    +--------------------+----------+
    |COR                 |QUANTIDADE|
    +--------------------+----------+
    |Vermelha            |12        |
    |Outros              |543       |
    |Amarela             |328       |
    |Branca              |17946     |
    |NULL                |16343     |
    |Parda               |6548      |
    |Preta               |1199      |
    +--------------------+----------+
    
    +--------+---------+-----------+
    |LATITUDE|LONGITUDE|OCORRENCIAS|
    +--------+---------+-----------+
    |-23.5285|-46.6698 |142        |
    |-23.6104|-46.4404 |57         |
    |-23.5502|-46.6327 |55         |
    |-23.5164|-46.6250 |53         |
    |-23.5505|-46.6343 |51         |
    |-23.5351|-46.6337 |50         |
    |-23.5119|-46.6126 |47         |
    |-23.5353|-46.6341 |46         |
    |-23.5496|-46.6139 |43         |
    |-23.5152|-46.6420 |43         |
    |-23.5747|-46.5029 |40         |
    |-23.5838|-46.6368 |37         |
    |-23.5230|-46.6878 |33         |
    |-23.5779|-46.6456 |32         |
    |-23.5443|-46.6326 |32         |
    |-23.5429|-46.4206 |30         |
    |-23.5270|-46.6637 |30         |
    |-23.5459|-46.6387 |29         |
    |-23.5417|-46.6367 |29         |
    |-23.5251|-46.6187 |28         |
    +--------+---------+-----------+
    only showing top 20 rows
    


### Finalizando o programa

Após os insumos serem extraidos,
iremos remover a persistencia do dataframe.


```scala
yearMonthRDO.unpersist()
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    res47: yearMonthRDO.type = [ID_DELEGACIA: string, NOME_DEPARTAMENTO: string ... 29 more fields]



```scala
println("✌🏻")
```


    VBox()



    FloatProgress(value=0.0, bar_style='info', description='Progress:', layout=Layout(height='25px', width='50%'),…


    ✌🏻



```scala

```
