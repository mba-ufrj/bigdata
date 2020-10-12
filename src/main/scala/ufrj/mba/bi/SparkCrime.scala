package ufrj.mba.bi

import java.time.LocalDate

import org.apache.spark.sql
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkCrime {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Crime spark")
      .master("local[*]")
      .getOrCreate()
    // Carregando o dataset de ocorrências.
    val crimeRDO = spark.read.option("header", "true").csv("s3://study-bi/rdo/rdo.csv")

    // Definindo as variaveis que serão utilizadas para execução recorrente, o ano da consulta e o mes da consulta.
    val yearSelect = LocalDate.now.getYear
    val monthSelect = LocalDate.now.getMonthValue - 1 // Analisando o mês anterior.
    val storagePath = yearSelect + "/" + monthSelect + "/"

    // Analise da tendencia da quantidade de crimes, se esta diminuindo ou nao, analisado os ultimos 6 meses
    //e armazenado o arquivo para apresentação.
    val crimeTrend = crimeRDO.filter(col("ANO_BO") === yearSelect &&
      month(to_date(col("DATA_OCORRENCIA_BO"), "dd/MM/yyyy")).lt(monthSelect + 1) &&
      month(to_date(col("DATA_OCORRENCIA_BO"), "dd/MM/yyyy")).gt(monthSelect - 6)).
      withColumn("MES", month(to_date(col("DATA_OCORRENCIA_BO"), "dd/MM/yyyy"))).
      groupBy("MES").
      agg(count("NUM_BO").as("OCORRENCIAS")).
      sort(asc("MES")).select("MES", "OCORRENCIAS")

    crimeTrend.write.mode(SaveMode.Overwrite).
      json("s3://study-bi/rdo/" + storagePath + "trend/")

    // Filtrando o RDO original e persistindo ele para futuras consultas.
    val yearMonthRDO = crimeRDO.filter(col("ANO_BO") === yearSelect &&
      month(to_date(col("DATA_OCORRENCIA_BO"), "dd/MM/yyyy")) === monthSelect).persist()

    // Pegando todos os estados que serão utilziados para consulta, no nosso caso somente, Sao Paulo.
    val state = yearMonthRDO.select(trim(col("NOME_MUNICIPIO_CIRC"))).dropDuplicates()

    // Convertando para lista para poder executar operacoes internas.
    // Para cada estado sera feita uma analise.
    state.collectAsList().forEach(st => {
      val stateName = st.get(0)
      // Feita a analise de quantidade de ocorencias por cidade do estado e armazenado o arquivo para apresentação.
      val monthCityOccurence = yearMonthRDO.filter(trim(col("NOME_MUNICIPIO_CIRC")) === stateName).
        groupBy("CIDADE").agg(count("NUM_BO").as("OCORRENCIAS")).
        select(trim(col("CIDADE")).as("CIDADE"), col("OCORRENCIAS")).
        sort(desc("OCORRENCIAS"))

      monthCityOccurence.write.mode(SaveMode.Overwrite).
        json("s3://study-bi/rdo/" + storagePath + stateName + "/city/")

      //       Inicio da analise do perfil das vitimas baseado no mes.
      //       Analise do sexo das vitimas e armazenado o arquivo para apresentação.
      val monthSexVictimProfile = yearMonthRDO.filter(trim(col("NOME_MUNICIPIO_CIRC")) === stateName)
        .select(col("SEXO_PESSOA").as("SEXO")).
        groupBy(col("SEXO")).count().withColumnRenamed("count", "QUANTIDADE")

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

      monthAgeVictimProfile.write.mode(SaveMode.Overwrite).
        json("s3://study-bi/rdo/" + storagePath + stateName + "/profile/age/")

      //       Analise da cor da pele das vitimas e armazenado o arquivo para apresentação.
      val monthSkinVictimProfile = yearMonthRDO.filter(trim(col("NOME_MUNICIPIO_CIRC")) === stateName)
        .select(col("COR_CUTIS").as("COR")).
        groupBy(col("COR")).count().withColumnRenamed("count", "QUANTIDADE")

      monthSkinVictimProfile.coalesce(1).write.mode(SaveMode.Overwrite).
        json("file:///Users/dalbuquerque/Desktop/mba/" + storagePath + stateName + "/profile/skin/")


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

      locationOccurence.write.mode(SaveMode.Overwrite).
        json("s3://study-bi/rdo/" + storagePath + stateName + "/location/")

    })

    yearMonthRDO.unpersist()
    spark.stop()

  }
}
