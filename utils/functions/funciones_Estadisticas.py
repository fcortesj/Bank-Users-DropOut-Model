import numpy as np
from numpy import zeros, ones

def Vector_medias(x):
    """
    devuelve las medias de cada columna
    tambien le llamo xbarra
    """
    xx= np.array(x)
    m,n = xx.shape
    unos = ones([m,1])
    medias = float(1.0/m)*xx.T.dot(unos) 
    return medias

def Matriz_datos_centrados(x):
    """
    A cada elemento de la matriz se le resta la media
    tambien la llamo xmono
    """
    xx = np.array(x)
    m,n = xx.shape
    unos = ones([m,1])
    xmono = x - unos.dot(Vector_medias(xx).T)
    return xmono

def matriz_P(x):
    """
    La matriz P es cuadrada simétrica e idempotente ()PP = P)
    Tiene rango (m-1) es ortogonal al espacio definido por el vector 1 ya que P*1 = 0 y proyecta los
    datos ortogonalmente al espacio deÞnido por el vector constante
    """
    xx = np.array(x)
    m,n = xx.shape
    unos = ones([m,1])
    Identidad = np.identity(m)
    P = Identidad - float(1.0/m) * unos.dot(unos.T)
    return P

def Matriz_covarianzas(x):
    """
    matriz de covarianzas a partir de los datos. OBSERVE que se divide por n
    semidefinida positiva o sea que   y' S y >= 0 para todo vector y
    Tambien la traza, el determinante y los valores propios de esta matriz son no negativos
    """
    xx = np.array(x)
    m,n = xx.shape
    P = matriz_P(x)
    intermedio =P.dot(xx)
    S = float(1.0/m)*(xx.T.dot(intermedio))
    return S

#   data.std(axis=0) # esta funcion divide por m 

def Matriz_covarianzasCorregida(x):
    """
    matriz de covarianzas a partir de los datos. OBSERVE que se divide por (n-1)
    np.cov(Data, rowvar=False) # esta funcion me da la covarianza corregida
    """
    xx = np.array(x)
    m,n = xx.shape
    P = matriz_P(x)
    intermedio =P.dot(xx)
    S = float(1.0/(m-1))*(xx.T.dot(intermedio))
    return S

def Variabilidad_Total(x):
    """
    medida de variablidad global 
    la variabilidad de un conjunto de variables es la traza de su matriz de varianzas y covarianzas
    """
    SS = Matriz_covarianzas(x)
    T = np.trace(SS)
    return T

def Varianza_promedio(x):
    """
    medida de variablidad global
    medida de variabilidad
    """
    xx = np.array(x)
    m,n = xx.shape
    VP = float(1.0/n)*Variabilidad_Total(xx)
    return VP


def Varianza_Generalizada(x):
    """
    medida de variablidad global
    es el determinante de la matriz de varianzas y covarianzas
    """
    SS = Matriz_covarianzas(x)
    VG = np.linalg.det(SS)
    return VG

def DesviacionTipca_Generalizada(x):
    """
    medida de variablidad global
    es una medida del area (n=2), volumen (n=3) o hipervolumen (n>3) ocupado por el conjunto de datos
    """
    vg = Varianza_Generalizada(x)
    DTG = math.sqrt(vg)
    return DTG


def is_positiva_definida(A):
    """
    se chequea si la matriz es simetrica y definida positiva
    usando la descomposicion de cholesky
    """
    if np.allclose(A, A.T):
        try:
            np.linalg.cholesky(A)
            return True
        except np.linalg.LinAlgError:
            return False
    else:
        return False 

def estandariza_matriz(x):
    """
    las variables estandarizadas multivariantemente como y = S^{-1/2}(x- x^barra)
    
    """
    Sdia = np.diag(Matriz_covarianzas(x))**0.5 #son las varianzas poblacionales
    estand = ones(Sdia.shape)/Sdia
    estandar = Matriz_datos_centrados(x)/Sdia
    return estandar
    
def correlaciones(x):
    """
    funcion para calcular las correlaciones entre pares de variables
    """
    Sdia = np.diag(Matriz_covarianzas(x))**-0.5 #son las varianzas poblacionales
    matriz_D = np.diag(Sdia,0) #matriz cuadrada diagonal cuya diagonal es Sdia
    intermedio = matriz_D.dot(Matriz_covarianzas(x))
    Correl = intermedio.dot(matriz_D)
    return Correl
