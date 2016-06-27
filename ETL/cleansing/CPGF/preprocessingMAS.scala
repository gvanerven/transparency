import java.security.MessageDigest;
import java.math.BigInteger;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.text.DateFormat;

def md5(s: String): String = {
    new BigInteger(1, MessageDigest.getInstance("MD5").digest(s.getBytes)).toString(16)    
}

sc.setLogLevel("ERROR")
//val fileCPGF = sc.textFile("CPGF/data/*", 2)

val fileCPGF = sc.textFile("file:///dados/workspace/gustavocgve/Projetos/CPGF/dados/*", 2)
fileCPGF.cache

val tmpFile = fileCPGF.filter(line => !(line.startsWith("C") && line.trim != "")).map(line => line.toUpperCase).map(line => line.split("\t"))

case class transacaoCPGFRAW(
    idPortador: String,
    idFavorecido: String,
    codigoOrgaoSuperior: String,
    nomeOrgaoSuperior: String,
    codigoOrgaoSubordinado: String,
    nomeOrgaoSubordinado: String,
    codigoUnidadeGestora: String,
    nomeUnidadeGestora: String,
    anoExtrato: String,
    mesExtrato: String,
    CPFPortador: String,
    nomePortador: String,
    tipoTransacao: String,
    dataTransacao: String,
    CNPJCPFFavorecido: String,
    nomeFavorecido: String,
    valorTransacao: String)

val transacoesRAW = tmpFile.map(e => transacaoCPGFRAW(
    e(8).trim + e(9).trim.replace(" ","").trim,
    if(e(10).trim.contains("SAQUE")) "OP00SAQUE" else e(12) + e(13).replace(" ","").trim,
    e(0).trim,
    e(1).trim,
    e(2).trim,
    e(3).trim,
    e(4).trim,
    e(5).trim,
    e(6).trim,
    e(7).trim,
    e(8).trim,
    e(9).trim,
    e(10).trim,
    e(11).trim,
    if(e(10).trim.contains("SAQUE")) "OP00" else e(12),
    if(e(10).trim.contains("SAQUE")) "SAQUE" else e(13),
    if(e(14).trim == "") "0" else e(14).trim.replace(",","."))).toDF

//transacoesRAW.filter(i => i.tipoTransacao.contains("SAQUE")).take(1)

transacoesRAW.where("tipoTransacao like 'SAQUE%'").take(1)
transacoesRAW.describe("valorTransacao").show
transacoesRAW.where("valorTransacao is null").count
transacoesRAW.where("valorTransacao = ''").count //Retriving 1 because one line was null (add 'line.trim != ""' to map())
transacoesRAW.describe("nomeFavorecido").show
transacoesRAW.describe("nomePortador").show
transacoesRAW.where("nomePortador = ''").count //245312
transacoesRAW.where("nomeFavorecido = ''").count //1976
transacoesRAW.describe("mesExtrato").show //complete
transacoesRAW.describe("anoExtrato").show //complete
transacoesRAW.describe("codigoOrgaoSuperior").show //complete
transacoesRAW.describe("nomeOrgaoSuperior").show //complete
transacoesRAW.describe("CPFPortador").show
transacoesRAW.where("CNPJCPFFavorecido = '' and tipoTransacao not like '%SAQUE%'").count //0
//CNPJCPFFavorecido is null for cash withdraw
transacoesRAW.where("CNPJCPFFavorecido = '' and tipoTransacao not like '%SAQUE%'").count //9884
transacoesRAW.where("CNPJCPFFavorecido = '' and nomeFavorecido = '' and tipoTransacao not like '%SAQUE%'").count //1513
transacoesRAW.where("CNPJCPFFavorecido = '' and nomeFavorecido <> '' and tipoTransacao not like '%SAQUE%'").count //8371

transacoesRAW.columns.foreach(transacoesRAW.describe(_).show)

transacoesRAW.where("idPortador <> '' and nomePortador = ''").count //0
transacoesRAW.where("idPortador = '' and nomePortador <> ''").count //0
//both are missing at the same time
transacoesRAW.where("idFavorecido = '' and nomeFavorecido <> ''").count //0
transacoesRAW.where("idFavorecido <> '' and nomeFavorecido = ''").count //463
//when the record doesn't have idFavorecido it doesn't have nomeFavorecido, but it can have id and doesn't have name 

//For T2 Multi-Agent Systems: Remove the confidential lines, create a fake id for cash withdraw, remove records where idPortador is null and 
//set to SEM-NOME where nomeFavorecido is null but idFavorecido isn't.
transacoesRAW.where("idPortador = ''").count //245312
transacoesRAW.where("CPFPortador = ''").count //245312

//codigoOrgaoSuperior: 0,
//nomeOrgaoSuperior: 1,
//codigoOrgaoSubordinado: 2,
//nomeOrgaoSubordinado: 3,
//codigoUnidadeGestora: 4,
//nomeUnidadeGestora: 5,
//anoExtrato: 6,
//mesExtrato: 7,
//CPFPortador: 8,
//nomePortador: 9,
//tipoTransacao: 10,
//dataTransacao: 11,
//CNPJCPFFavorecido: 12,
//nomeFavorecido: 13,
//valorTransacao: 14


val finalFile = fileCPGF.map(line => line.replace("\"", "").replace("'", "").toUpperCase).
    filter(line => (!line.startsWith("C") && line.trim != "" && !line.contains("INFORMAÇÕES PROTEGIDAS POR SIGILO, NOS TERMOS DA LEGISLAÇÃO"))).
    map(line => line.split("\t")).filter(l => l(8).trim != "")

finalFile.cache

val transacoesTESTE = finalFile.map(e => transacaoCPGFRAW(
    md5(e(8).trim + e(9).trim.replace(" ","").trim),
    if(e(10).trim.contains("SAQUE")){
        md5("OP00SAQUE")
    } else {
        var sFinal = ""
        if(e(12).trim != ""){ 
            sFinal = e(12).trim
        }else {
            sFinal = "FALTANDO"
        }
        if(e(13).trim != ""){ 
            sFinal += e(13).replace(" ","").trim
        }else{
            sFinal += "FALTANDO"
        }
        md5(sFinal)
    },
    e(0).trim,
    e(1).trim,
    e(2).trim,
    e(3).trim,
    e(4).trim,
    e(5).trim,
    e(6).trim,
    e(7).trim,
    e(8).trim,
    e(9).trim,
    e(10).trim,
    e(11).trim,
    if(e(10).trim.contains("SAQUE")) "OP00" else if(e(12).trim == "") "FALTANDO" else e(12).replace("'","").trim,
    if(e(10).trim.contains("SAQUE")) "SAQUE" else if(e(13).trim == "") "FALTANDO" else e(13).replace("\"", "").trim,
    if(e(14).trim == "") "0" else e(14).trim.replace(",","."))).toDF

transacoesTESTE.columns.foreach(transacoesTESTE.describe(_).show)

//data.split("/")(0)
//var cal = new GregorianCalendar(2015, 1, 2)
//cal.set(Calendar.MONTH, data.split("/")(1).toInt)
//Final class
val finalFile = fileCPGF.map(line => line.replace("\"", "").replace("'", "").toUpperCase).
    filter(line => (!line.startsWith("C") && line.trim != "" && !line.contains("INFORMAÇÕES PROTEGIDAS POR SIGILO, NOS TERMOS DA LEGISLAÇÃO"))).
    map(line => line.split("\t")).filter(l => l(8).trim != "")

case class transacaoCPGF(
    idPortador: String,
    idFavorecido: String,
    codigoOrgaoSuperior: Int,
    nomeOrgaoSuperior: String,
    codigoOrgaoSubordinado: Int,
    nomeOrgaoSubordinado: String,
    codigoUnidadeGestora: Int,
    nomeUnidadeGestora: String,
    anoExtrato: Int,
    mesExtrato: Int,
    CPFPortador: String,
    nomePortador: String,
    tipoTransacao: String,
    dataTransacao: String,
    CNPJCPFFavorecido: String,
    nomeFavorecido: String,
    valorTransacao: Double)

val transacoesFINAL = finalFile.map(e => transacaoCPGF(
    md5(e(8).trim + e(9).trim.replace(" ","").trim),
    if(e(10).trim.contains("SAQUE")){
        md5("OP00SAQUE")
    } else {
        var sFinal = ""
        if(e(12).trim != ""){ 
            sFinal = e(12).trim
        }else {
            sFinal = "FALTANDO"
        }
        if(e(13).trim != ""){ 
            sFinal += e(13).replace(" ","").trim
        }else{
            sFinal += "FALTANDO"
        }
        md5(sFinal)
    },
    e(0).trim.toInt,
    e(1).trim,
    e(2).trim.toInt,
    e(3).trim,
    e(4).trim.toInt,
    e(5).trim,
    e(6).trim.toInt,
    e(7).trim.toInt,
    e(8).trim,
    e(9).replace("\"", "").trim,
    e(10).trim,
    e(11).trim.split("/")(2) + "-" + e(11).trim.split("/")(1) + "-" +  e(11).trim.split("/")(0),
    if(e(10).trim.contains("SAQUE")) "OP00" else if(e(12).trim == "") "FALTANDO" else e(12).trim,
    if(e(10).trim.contains("SAQUE")) "SAQUE" else if(e(13).trim == "") "FALTANDO" else e(13).replace("\"", "").trim,
    if(e(14).trim == "") 0.0 else e(14).trim.replace(",",".").toDouble )).toDF

//transacoesFINAL.write.mode("overwrite").partitionBy("anoExtrato").parquet("CPGF/spark/transacoes")
transacoesFINAL.write.mode("overwrite").partitionBy("anoExtrato").parquet("CPGF/spark/transacoes")
transacoesFINAL.write.mode("overwrite").json("file:////home/gvanerven/Proj/CPGF/spark/data")
//mongoimport -db cpgf --collection portadoresSMA --file docs.json