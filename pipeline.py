import pyspark
import pandas as pd
import math


def conta_palavras_em_doc(item):
    url, text = item
    words = text.strip().split()
    return [(word.lower(), 1) for word in set(words)]

def junta_contagens(nova_contagem, contagem_atual):
    return nova_contagem + contagem_atual

def computa_relevancia(item):
    palavra, valor = item
    freq, idf = valor
    relevancia = freq*idf
    return (palavra, relevancia)
    
def conta_palavras_total(item):
    url, text = item
    words = text.strip().split()
    words = [w for w in words if w.isalpha()]
    words_filtered = [w.lower() for w in words if len(w) > 3]
    return [(word.lower(), 1) for word in words_filtered]

def filtra_doc_freq(item):
    palavra, contagem = item
    return (contagem < DOC_COUNT_MAX) and (contagem >= DOC_COUNT_MIN)

def sort_100(item): 
    palavra, relevancia = item
    return 

def computa_idf(item):
    palavra, contagem = item
    idf = math.log10(N / contagem)
    return(palavra, idf)

def computa_freq(item):
    palavra, contagem = item
    freq = math.log10(1+contagem)
    return(palavra, freq)

def encontra_palavra(item, palavra):
    url, text = item
    words = text.strip().split()
    words_list = [word.lower() for word in words]
    if palavra in words_list:
        return [item]
    return []

def encontra_palavra1(item):
    return encontra_palavra(item, palavra1)

def encontra_palavra2(item):
    return encontra_palavra(item, palavra2)

def conta_palavras_local(item):
    url, text = item
    words = text.strip().split()
    words = [w for w in words if w.isalpha()]
    filtered_words = []
    for i in range(len(words)):
        if i < 5:
            if palavra1 in words[: i + 5] or palavra2 in words[: i + 5]:
                filtered_words.append(words[i])
        elif i > len(words) - 5:
            if palavra1 in words[i - 5 :] or palavra2 in words[i - 5 :]:
                filtered_words.append(words[i])
        else:
            if (palavra1 in words[i - 5 : i + 5] or palavra2 in words[i - 5 : i + 5]):
                filtered_words.append(words[i])

    filtered_words = [w.lower() for w in filtered_words if len(w) > 3]
    return [(w.lower(), 1) for w in filtered_words]


sc = pyspark.SparkContext(appName="alexandria")

sc.parallelize(range(1000)).count()

rdd = sc.sequenceFile("s3://megadados-alunos/web-brasil") #rdd = sc.textFile("web_brasil_small/part-00000")

N = rdd.count()

DOC_COUNT_MIN = 10
DOC_COUNT_MAX = N * 0.7

#Palavras de estudo
palavra1 = 'boulos'
palavra2 = 'covas'

#Filtragem das palavras
rdd_palavra1 = rdd.flatMap(encontra_palavra1)
rdd_palavra2 = rdd.flatMap(encontra_palavra2)

#Interseção
rdd_palavra_intersection = rdd_palavra1.intersection(rdd_palavra2)

rdd_doc_count_intersection = rdd.flatMap(conta_palavras_em_doc).reduceByKey(junta_contagens).filter(filtra_doc_freq)
rdd_idf = rdd.flatMap(conta_palavras_total).reduceByKey(junta_contagens)
rdd_idf = rdd_idf.map(computa_idf)

rdd_words_count = rdd_palavra_intersection.flatMap(conta_palavras_total).reduceByKey(junta_contagens)
rdd_words_freq = rdd_words_count.map(computa_freq)

rdd_join = rdd_words_freq.join(rdd_idf)
rdd_relevancia = rdd_join.map(computa_relevancia)
list_relevance = rdd_relevancia.takeOrdered(100, key=lambda x: -x[1])

#Palavra 1
rdd_words_count_palavra1 = rdd_palavra1.flatMap(conta_palavras_total).reduceByKey(junta_contagens)
rdd_words_freq_palavra1 = rdd_words_count_palavra1.map(computa_freq)

rdd_join_palavra1 = rdd_words_freq_palavra1.join(rdd_idf)
rdd_relevancia_palavra1 = rdd_join_palavra1.map(computa_relevancia)
list_relevance_palavra1 = rdd_relevancia_palavra1.takeOrdered(100, key=lambda x: -x[1])

#Palavra 2
rdd_words_count_palavra2 = rdd_palavra2.flatMap(conta_palavras_total).reduceByKey(junta_contagens)
rdd_words_freq_palavra2 = rdd_words_count_palavra2.map(computa_freq)

rdd_join_palavra2 = rdd_words_freq_palavra2.join(rdd_idf)
rdd_relevancia_palavra2 = rdd_join_palavra2.map(computa_relevancia)
list_relevance_palavra2 = rdd_relevancia_palavra2.takeOrdered(100, key=lambda x: -x[1])

#===========================================================================================================
#Palavras Locais
#===========================================================================================================

#Interseção
rdd_words_count_local = rdd_palavra_intersection.flatMap(conta_palavras_local).reduceByKey(junta_contagens)
rdd_words_freq_local = rdd_words_count_local.map(computa_freq)

rdd_join_local = rdd_words_freq_local.join(rdd_idf)
rdd_relevancia_local = rdd_join_local.map(computa_relevancia)
list_relevance_local = rdd_relevancia_local.takeOrdered(100, key=lambda x: -x[1])

#Palavra 1
rdd_words_count_palavra1_local = rdd_palavra1.flatMap(conta_palavras_local).reduceByKey(junta_contagens)
rdd_words_freq_palavra1_local = rdd_words_count_palavra1_local.map(computa_freq)

rdd_join_palavra1_local = rdd_words_freq_palavra1_local.join(rdd_idf)
rdd_relevancia_palavra1_local = rdd_join_palavra1_local.map(computa_relevancia)
list_relevance_palavra1_local = rdd_relevancia_palavra1_local.takeOrdered(100, key=lambda x: -x[1])

#Palavra 2
rdd_words_count_palavra2_local = rdd_palavra2.flatMap(conta_palavras_local).reduceByKey(junta_contagens)
rdd_words_freq_palavra2_local = rdd_words_count_palavra2_local.map(computa_freq)

rdd_join_palavra2_local = rdd_words_freq_palavra2_local.join(rdd_idf)
rdd_relevancia_palavra2_local = rdd_join_palavra2_local.map(computa_relevancia)
list_relevance_palavra2_local = rdd_relevancia_palavra2_local.takeOrdered(100, key=lambda x: -x[1])

#Cria dfs
df_palavra1 = pd.DataFrame(list_relevance_palavra1, columns =['Palavra', 'Relevância']) 
df_palavra2 = pd.DataFrame(list_relevance_palavra2, columns =['Palavra', 'Relevância']) 
df_palavra_intersect = pd.DataFrame(list_relevance, columns =['Palavra', 'Relevância'])

df_palavra1_local = pd.DataFrame(list_relevance_palavra1_local, columns =['Palavra', 'Relevância']) 
df_palavra2_local = pd.DataFrame(list_relevance_palavra2_local, columns =['Palavra', 'Relevância']) 
df_palavra_intersect_local = pd.DataFrame(list_relevance_local, columns =['Palavra', 'Relevância'])

#Cria csvs
df_palavra1.to_csv("s3://megadados-alunos/luvi_lucax/ResultadoPalavra1.csv")
df_palavra2.to_csv("s3://megadados-alunos/luvi_lucax/ResultadoPalavra2.csv")
df_palavra_intersect.to_csv("s3://megadados-alunos/luvi_lucax/ResultadoIntersect.csv")

df_palavra1_local.to_csv("s3://megadados-alunos/luvi_lucax/ResultadoPalavra1Local.csv")
df_palavra2_local.to_csv("s3://megadados-alunos/luvi_lucax/ResultadoPalavra2Local.csv")
df_palavra_intersect_local.to_csv("s3://megadados-alunos/luvi_lucax/ResultadoIntersectLocal.csv")

sc.stop()


