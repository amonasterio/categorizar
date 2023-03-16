#Fuente: https://colab.research.google.com/drive/1sw3NQDXQosRRNz-g_4woucNAVdufz94m#scrollTo=HXyZLBOwPgVV
import openai
import pandas as pd
import datetime
import logging
import argparse

logging.basicConfig(filename='example.log', encoding='utf-8', level=logging.DEBUG)
inicio=datetime.datetime.now()
logging.info(f"Hora ejecucion: {inicio}")
parser = argparse.ArgumentParser()
parser.add_argument('--api_key', help='API KEY OPEN AI')
parser.add_argument('--idioma', help='Idioma del keyword research')
parser.add_argument('--f_kws', help='Fichero con las keywords a categorizar')
parser.add_argument('--f_cat', help='Fichero con las categorías (si se desean utilizar)')
parser.add_argument('--n_col_kw', help='Nombre de la columna con las keywords del fichero de keywords')

args = parser.parse_args()

openai.api_key = args.api_key
IDIOMA=args.idioma
logging.info(f"Idioma: {IDIOMA}")
MODEL="gpt-3.5-turbo"
TEMPERATURE=0.6
MAX_TOKENS=1500
SEPARADOR=";"
LOTE=50 #bloques de keywords a categorizar en bloque
PROMPT_SYSTEM=f"Eres un asistente experto en agrupar palabras clave (en {IDIOMA}) en categorías"
f_salida='salida.csv'
n_file_kw=args.f_kws
logging.info(f"Ruta fichero keywords: {n_file_kw}")
n_file_cat=args.f_cat
logging.info(f"Ruta fichero categorías: {n_file_cat}")
n_columna_kw=args.n_col_kw
logging.info(f"Nombre columna keywords: {n_columna_kw}")
tenemos_categorias=len(n_file_cat)>0
logging.info(f"Tenemos categorías predefinidas: {tenemos_categorias}")



#Elimina de la lista de categorías los elementos que no deben estar, así como deja las cateorías sin guiones
def limpiarListaCategorias(lista):
    lista_final=[]
    for i in lista:
       if i.startswith("- "):
          lista_final.append(i.replace("- ",""))   
    return lista_final

#Cuando separamos keyword y categoría. Si se han asignado dos o más categorías, dejamos únicamente una
def dejaUnaCategoria(cadena, separador):
  num=cadena.count(separador)
  #si hay más de un separador eliminamos lo que hay del segundo separador en adelante
  if num>1:
    pos_1=cadena.find(separador)
    pos_2=cadena.find(separador,pos_1+1)
    cadena=cadena[:pos_2]
  return cadena


#Ejecuta una consulta a Chat GPT y devuelve en formato lista
def ejecutaConsulta(prompt_consulta,temperatura):
    try:
        categorizacion=openai.ChatCompletion.create(
        model=MODEL,
        max_tokens=MAX_TOKENS,
        temperature=temperatura,
        messages=[
                {"role": "system", "content": PROMPT_SYSTEM},
                {"role": "user", "content": prompt_consulta},
            ]
        )
        # Separamos las categorías que GPT ha devuelto
        listado = categorizacion.choices[0].message.content.split('\n')
    except Exception as err:
        logging.error(f"Unexpected {err=}, {type(err)=}")

    return listado

#leemos las categorías de un csv sin cabecera
def leerCategorias(nombre_fichero):
   # Leemos el archivo CSV con las categorías
    df_cat = pd.read_csv(nombre_fichero,header=None)
    # Convertimos el contenido del DataFrame a una cadena de texto en formato CSV
    lista_cat=df_cat[0].values.tolist()
    return lista_cat

#leemos las kewyords de un csv con cabecera
def leerKeywords(nombre_fichero, nombre_columna_kw):
   # Leemos el archivo CSV con las keywords
    df_kw = pd.read_csv(nombre_fichero)
    # Convertimos el contenido del DataFrame a una cadena de texto en formato CSV
    lista_kw=df_kw[nombre_columna_kw].values.tolist()
    return lista_kw

#Busca los elementos en lista2 que no están presentes en lista1. Devuelva una lista con ausentes
def encontrarElementosAusentes(lista1,lista2):
    lista_ausentes=[]
    if len(lista2)>len(lista1):
        i=0
        while i<len(lista2):
            elemento=lista2[i]
            if not elemento in lista1:
                lista_ausentes.append(elemento)
            i+=1
    return lista_ausentes

#Elimina los elementos duplicados de una lista
def eliminaDuplicadosLista(lista):
  unicos = list(set(lista))
  return unicos



# Categoriza la lista de keywords con las categorías de lista_cat
# Devuelve una lista separada por SEPARADOR
def categoriza(lista_keywords,lista_cat):
    num_kw=len(lista_keywords)
    # Inicializamos el array donde guardaremos las keywords categorizadas
    keywords_categorizadas = []

    # Crea un contador de keywords procesadas y un índice de lote inicial
    contador_keywords = 0
    indice_lote = 0

    # Mientras el contador sea menor al número total de keywords en el archivo CSV
    while contador_keywords < num_kw:
        logging.debug(indice_lote)
        # Obtener el lote de x keywords
        lotes_keywords = lista_keywords[indice_lote:indice_lote+LOTE]

        # Convertimos la lista a cadena
        lote_string = '\n'.join(lotes_keywords)
    
        prompt = f'Para el siguiente listado de palabras clave (en {IDIOMA}):\n\n{lote_string}\n\nasigna a cada una de las keywords únicamente una de estas categorías:\n\n{lista_cat}\n\nSi alguna keyword no encaja en ninguna categoría, dejarla vacía\nEl formato de salida deber ser tipo csv separado por {SEPARADOR}'

        keywords_categorizadas_temp =  ejecutaConsulta(prompt,TEMPERATURE)
        #Dejamos en el log la asignación de categorías
        for elemento in keywords_categorizadas_temp:
            logging.info(elemento)

        keywords_categorizadas+= keywords_categorizadas_temp

        # Aumenta el contador de keywords procesadas y el índice de lote
        contador_keywords += len(lotes_keywords)
        indice_lote += LOTE
    return keywords_categorizadas

#Genera un dataFrame de keywords categorizadas recibiendo la lisa con la categorización
def generaDfKeywordsCategorizadas(kws_categorizadas):  
    categorias = []
    keywords = []    
    for elemento in kws_categorizadas:
        if SEPARADOR in elemento:
            #Por si ha asignado más de una categoría dejamos sólo una
            elemento=dejaUnaCategoria(elemento,SEPARADOR)
            keyword, categoria = elemento.split(SEPARADOR)
            if (categoria != '' and keyword != ''):
              categorias.append(categoria)
              keywords.append(keyword)
    df = pd.DataFrame({n_columna_kw: keywords,'Categoría': categorias})
    return df

inicio=datetime.datetime.now()
# Inicializamos el array donde guardaremos las categorías de las keywords
categorias_keywords = []




#leemos el csv con las keywords
lista_kw=leerKeywords(n_file_kw,n_columna_kw)
#quitamos duplicados
lista_kw=eliminaDuplicadosLista(lista_kw)
# Calcular el numero total de keywords
num_keywords = len(lista_kw)

if tenemos_categorias:
    categorias_keywords = leerCategorias(n_file_cat)
else:
    # Generamos las caterorías a partir de las keywords que tenemos
    # Crea un contador de keywords procesadas y un índice de lote inicial
    contador_keywords = 0
    indice_lote = 0
    # Mientras el contador sea menor al número total de keywords en el archivo CSV
    while contador_keywords < num_keywords:
        # Obtener el lote de x keywords
        lotes_keywords = lista_kw[indice_lote:indice_lote+LOTE]
        # Convertimos la lista a cadena
        lote_string = '\n'.join(lotes_keywords)
        # Si es la primera iteración del bucle, pedimos a GPT3 que cree un listado de categorías detalladas
        # para las keywords del lote
        if len(categorias_keywords) == 0:
            prompt=f"Olvida cualquier consulta realizada previamente. Tengo el objetivo de agrupar palabras clave en categorías generales. Necesito que para el siguiente listado de palabras clave (en {IDIOMA}), crees categorías semánticas (en {IDIOMA}) en las que puedan encajar. Quiero que devuelvas únicamente en cada línea los nombres de las categorías creadas empezando por -: \n\n{lote_string}"
        # Si no es la primera iteración del bucle, pedimos a GPT que categorice las keywords del lote, pero antes
        # de asignar una categoría nueva, revisamos que no esté en categorias_keywords
        else:
            prompt = f"Tengo el objetivo de agrupar palabras clave en categorías generales. Quiero que intentes agrupar las siguientes palabras clave (en {IDIOMA}):\n{lote_string}\n\nen las siguientes categorías:\n{categorias_keywords}\n\nSi ves que no encajan en ninguna de ellas crea una nueva categoría. A continuación, lista únicamente el nombre de todas las categorías nuevas que se hayan creado empezando por -"
        
         # Ejecutamos la consutlta a Chat GPT
        categorias_keywords_temp = ejecutaConsulta(prompt,TEMPERATURE)
        
        #Filtramos la respuesta para dejar únicamente las categorías
        categorias_keywords+= limpiarListaCategorias(categorias_keywords_temp)

        #Eliminamos duplicados
        categorias_keywords=eliminaDuplicadosLista(categorias_keywords)
        #Para evitar que se generen demasiadas categorías y tengamos problemas con exceso de tokens
        if len(categorias_keywords)>70:
            #Simplificamos el listado de categorías
            prompt=f'Quiero que simplifiques el siguiente listado de categorías:\n{categorias_keywords}\n\nLa idea es renombrar categorías que son semánticamente similares. Sólo quiero que muestres el nombre de las catetorías finales empezando cada línea de categoría por -'
            categorias_keywords=limpiarListaCategorias(ejecutaConsulta(prompt,0.3))

        # Aumenta el contador de keywords procesadas y el índice de lote
        contador_keywords += len(lotes_keywords)
        indice_lote += LOTE
        

    #Simplificamos el listado de categorías
    prompt=f'Quiero que simplifiques el siguiente listado de categorías:\n{categorias_keywords}\n\nLa idea es renombrar categorías que son semánticamente similares. Sólo quiero que muestres el nombre de las catetorías finales empezando cada línea de categoría por -'
    categorias_keywords=limpiarListaCategorias(ejecutaConsulta(prompt,0.3))
    #Dejamos en el log la asignación de categorías
    logging.info("Categorías creadas:")
    for elemento in categorias_keywords:
        logging.info(elemento)


# -------------------------
# Ahora ya tenemos las categorías creadas, vamos a pasar una segunda vez por el listado de keywords para categorizar todas en las categorías creadas

# Inicializamos el array donde guardaremos las keywords categorizadas
keywords_categorizadas = []

keywords_categorizadas=categoriza(lista_kw,categorias_keywords)

df_categorizacion=generaDfKeywordsCategorizadas(keywords_categorizadas)
df_categorizacion.to_csv('parcial.csv',index=False)

#Comprobamos si se ha quedad alguna keyword sin categorizar. Intentamos categorizarlas en 5 intentos
keywords=df_categorizacion[n_columna_kw].values.tolist()
if len(keywords)<num_keywords:
    logging.info("Faltan keywords que categorizar")
    repet=5
    i=0
    ausentes=encontrarElementosAusentes(keywords,lista_kw)
    while i<repet and len(ausentes)>0:
        logging.debug(f"Repetición: {i}")
        categorias = []
        keywords = [] 
        kws_ausentes_cat=categoriza(ausentes,categorias_keywords)
        df_temp=generaDfKeywordsCategorizadas(kws_ausentes_cat)
        df_categorizacion=pd.concat([df_categorizacion,df_temp],axis=0)
        keywords=df_categorizacion[n_columna_kw].values.tolist()
        ausentes=encontrarElementosAusentes(keywords,lista_kw)
        i+=1
    
df_categorizacion.to_csv(f_salida,index=False)
fin=datetime.datetime.now()
logging.info("Tiempo de ejecución: "+str(fin-inicio))
logging.info(f"Fin ejecucion: {fin}")


'''
#Metemos las categorías en el csv de semrush
df_kw = pd.read_csv(n_file_kw)
df_nuevo=df_kw[[n_columna_kw,'Search Volume','Keyword Difficulty','CPC','URL']]
df_final = df_nuevo.merge(df_categorizacion, on=n_columna_kw, how='left')
df_final.to_csv('final.csv', index=False)
'''



