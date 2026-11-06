
# Trabajo de Consulta: Funciones Anónimas

* **Materia:** Programación Funcional
* **Modalidad:** Individual (Entrega Semana 5)
* **Lenguajes:** Scala, Java, Python

---

## Índice
1. [Parte 1: Explorar — La programación funcional en el mundo real](#parte-1-explorar--la-programación-funcional-en-el-mundo-real)
2. [Parte 2: Entender — ¿Qué es una función anónima?](#parte-2-entender--qué-es-una-función-anónima)
3. [Parte 3: Aplicar — Tipos de problemas con funciones anónimas](#parte-3-aplicar--tipos-de-problemas-con-funciones-anónimas)
4. [Parte 4: Ampliar — Cuatro operaciones nuevas en Scala](#parte-4-ampliar--cuatro-operaciones-nuevas-en-scala)
5. [Parte 5: Comparar y Concluir](#parte-5-comparar-y-concluir)
6. [Instrucciones para ejecutar el código](#instrucciones-para-ejecutar-el-código)
7. [Referencias](#referencias)

---

## Parte 1: Explorar — La programación funcional en el mundo real

### Caso 1: Apache Spark (Procesamiento distribuido con Scala)
* **Problema que resuelve:** Procesamiento tolerante a fallos de grandes volúmenes de datos (Big Data) en clústeres.
* **Lenguaje o herramienta:** Scala (arquitectura de RDDs y DataFrames).
* **Ventaja del enfoque funcional:** La inmutabilidad de los datos permite paralelizar el cálculo en miles de núcleos sin bloqueos ni condiciones de carrera (*race conditions*). Utiliza además *lazy evaluation*: las lambdas aplicadas a colecciones no computan de inmediato, sino que forman un grafo acíclico dirigido (DAG) que Spark optimiza antes de la ejecución física.
* **Fragmento de código real:**
  ```scala
  val logsErrores = sc.textFile("hdfs://cluster/logs/*.log")
    .filter(linea => linea.contains("ERROR 500"))
    .map(linea => (linea.split(" ")(0), 1))
    .reduceByKey((acc, n) => acc + n)
  ```
  *Explicación:* `linea => linea.contains("ERROR 500")` es una función anónima que se serializa y envía a través de la red a cada nodo de trabajo para filtrar en paralelo.
* **Fuente consultada:** Documentación oficial de Apache Spark: *RDD Programming Guide* (https://spark.apache.org/docs/latest/rdd-programming-guide.html).

### Caso 2: Java Streams en Sistemas Empresariales (Microservicios en Producción)
* **Problema que resuelve:** Procesamiento y transformación de flujos transaccionales y entidades financieras en servicios REST sin mutación de estado.
* **Lenguaje o herramienta:** Java 17+ con Streams API.
* **Ventaja del enfoque funcional:** Permite escribir lógica de negocio declarativa en canalizaciones legibles (`filter -> map -> reduce`), evitando bucles tradicionales con contadores o listas intermedias mutables susceptibles a errores de concurrencia.
* **Fragmento de código real:**
  ```java
  List<String> clientesMorosos = transacciones.stream()
      .filter(t -> t.getDiasAtraso() > 90)
      .map(Transaccion::getCedulaCliente)
      .distinct()
      .toList();
  ```
  *Explicación:* La lambda `t -> t.getDiasAtraso() > 90` encapsula la regla de selección en línea como argumento de orden superior sin requerir clases anónimas verbosas.
* **Fuente consultada:** Oracle Java SE Documentation: *Aggregate Operations Tutorial* (https://docs.oracle.com/javase/tutorial/java/javaOO/lambdaexpressions.html).

---

## Parte 2: Entender — ¿Qué es una función anónima?

### 1. ¿Qué es y de dónde viene el nombre lambda?
Una **función anónima** es un literal de función: una expresión ejecutable que acepta parámetros y computa un valor sin estar ligada a un identificador formal (nombre). 

El término **lambda** proviene del **Cálculo Lambda ($\lambda$-calculus)** desarrollado por el matemático **Alonzo Church** en la década de 1930 para formalizar la computabilidad matemática. Church utilizó la letra griega $\lambda$ para denotar los parámetros ligados en una expresión matemática.

### 2. Diferencia frente a una función nombrada
Una función nombrada se registra explícitamente en el espacio de nombres para ser reutilizada a lo largo del programa, mientras que una lambda se define *in situ* para resolver un paso de cálculo puntual.

* **Función nombrada en Scala:**
  ```scala
  def duplicar(x: Int): Int = x * 2
  List(1, 2, 3).map(duplicar)
  ```
* **Misma lógica con función anónima:**
  ```scala
  List(1, 2, 3).map(x => x * 2)
  ```

### 3. Funciones como ciudadanos de primera clase
Significa que las funciones reciben el mismo estatus que cualquier tipo de dato fundamental (como `Int` o `String`):
* Pueden asignarse a variables.
* Pueden pasarse como argumentos a otras funciones.
* Pueden ser retornadas por funciones.

Esto permite que métodos como `map` o `filter` reciban funciones anónimas como parámetros para gobernar el comportamiento de la transformación o selección.

### 4. ¿Qué es un closure?
Un **closure** (cierre léxico) es una función anónima que retiene el acceso a variables declaradas fuera de su cuerpo (en su entorno circundante), incluso cuando la lambda es ejecutada en otro ámbito.

```scala
val umbralMinimo = 10
// La función anónima captura la variable externa 'umbralMinimo'
val esSuficiente = (cantidad: Int) => cantidad >= umbralMinimo
```

---

## Parte 3: Aplicar — Tipos de problemas con funciones anónimas

Para evitar repetición y asegurar coherencia, se usa la misma colección de 10 productos en los tres lenguajes:

| ID | Nombre | Categoría | Precio | Stock |
|:---|:---|:---|:---|:---|
| 1 | Pastillas de Freno | Frenos | $25.0$ | 15 |
| 2 | Disco de Freno | Frenos | $45.0$ | 8 |
| 3 | Amortiguador | Suspension | $65.0$ | 4 |
| 4 | Espiral Trasero | Suspension | $50.0$ | 6 |
| 5 | Filtro de Aceite | Motor | $8.0$ | 30 |
| 6 | Filtro de Aire | Motor | $12.0$ | 20 |
| 7 | Bujia Iridium | Motor | $10.0$ | 40 |
| 8 | Kit de Embrague | Transmision | $120.0$ | 3 |
| 9 | Aceite 10W40 | Lubricantes | $28.0$ | 18 |
| 10 | Refrigerante | Refrigeracion | $15.0$ | 12 |

---

### Problema 1: Selección (`filter`)
* **Situación real:** Un taller automotriz necesita detectar los repuestos con stock crítico (menos de 5 unidades) para solicitar reabastecimiento urgente a compras.
* **Pregunta que responde:** ¿Qué elementos cumplen una condición?
* **Soluciones:**
  * **Scala:** `catalogo.filter(p => p.stock < 5)` (ver `scala/Soluciones.scala`)
  * **Java:** `catalogo.stream().filter(p -> p.stock() < 5).toList()` (ver `java/Soluciones.java`)
  * **Python:** `list(filter(lambda p: p.stock < 5, catalogo))` (ver `python/soluciones.py`)
* **Salida de las tres versiones:**
  ```text
  - Amortiguador (Stock: 4)
  - Kit de Embrague (Stock: 3)
  ```

---

### Problema 2: Transformación (`map`)
* **Situación real:** Se requiere generar las etiquetas de facturación con el nombre en mayúsculas y el precio final con el 15% de IVA incluido.
* **Pregunta que responde:** ¿Cómo convierto cada elemento en otro valor?
* **Soluciones:**
  * **Scala:** `catalogo.map(p => f"${p.nombre.toUpperCase}%s -> $$${p.precio * 1.15}%.2f")`
  * **Java:** `catalogo.stream().map(p -> p.nombre().toUpperCase() + " -> $" + String.format(Locale.US, "%.2f", p.precio() * 1.15)).toList()`
  * **Python:** `list(map(lambda p: f"{p.nombre.upper()} -> ${p.precio * 1.15:.2f}", catalogo))`
* **Salida de las tres versiones (primeros 3 elementos):**
  ```text
  PASTILLAS DE FRENO -> $28.75
  DISCO DE FRENO -> $51.75
  AMORTIGUADOR -> $74.75
  ```

---

### Problema 3: Extremos (`maxBy`)
* **Situación real:** El departamento de seguros necesita conocer cuál es el producto de mayor valor comercial en bodega para fijar la póliza.
* **Pregunta que responde:** ¿Cuál es el mejor o el peor según un criterio?
* **Soluciones:**
  * **Scala:** `catalogo.maxBy(p => p.precio)`
  * **Java:** `catalogo.stream().max(Comparator.comparingDouble(p -> p.precio())).orElseThrow()`
  * **Python:** `max(catalogo, key=lambda p: p.precio)`
* **Salida de las tres versiones:**
  ```text
  Kit de Embrague ($120.0)
  ```

---

### Problema 4: Acumulación (`foldLeft`)
* **Situación real:** Se debe calcular el valor patrimonial total de la bodega multiplicando el precio de cada repuesto por su stock acumulado.
* **Pregunta que responde:** ¿Qué valor resume la lista?, ¿cómo evoluciona paso a paso?
* **Soluciones:**
  * **Scala:** `catalogo.foldLeft(0.0)((acc, p) => acc + (p.precio * p.stock))`
  * **Java:** `catalogo.stream().reduce(0.0, (acc, p) -> acc + (p.precio() * p.stock()), Double::sum)`
  * **Python:** `reduce(lambda acc, p: acc + (p.precio * p.stock), catalogo, 0.0)`
* **Salida de las tres versiones:**
  ```text
  Total inventario: $2419.00
  ```

---

## Parte 4: Ampliar — Cuatro operaciones nuevas en Scala

Las siguientes 4 operaciones pertenecen a la API de colecciones de Scala y son diferentes a las vistas en clase:

### 1. Ficha: `sortBy` (Familia: Ordenar)
1. **Qué hace:** Ordena los elementos de la lista en función del resultado devuelto por una función de proyección.
2. **Firma y lambda:** Recibe `f: A => B` (donde `B` tiene orden natural implícito `Ordering[B]`). Devuelve la colección ordenada.
3. **Ejemplo en Scala:**
   ```scala
   val porPrecio = catalogo.sortBy(p => p.precio)
   ```
   *Salida:* `Filtro de Aceite ($8.0), Bujia Iridium ($10.0), ... Kit de Embrague ($120.0)`
4. **Problema que resuelve:** Visualización de listas priorizadas o catálogos ordenados por precio ascendente. Se relaciona con la familia de **Extremos**.

### 2. Ficha: `groupBy` (Familia: Agrupar)
1. **Qué hace:** Agrupa los elementos en un mapa asociativo (`Map[K, List[A]]`) utilizando como llave el valor que produce la función anónima.
2. **Firma y lambda:** Recibe `f: A => K`. Devuelve un `Map` donde cada clave `K` apunta a su lista de elementos.
3. **Ejemplo en Scala:**
   ```scala
   val porCategoria = catalogo.groupBy(p => p.categoria)
   ```
   *Salida:* `Map(Frenos -> List(Pastillas, Disco), Motor -> List(Filtro Aceite, ...))`
4. **Problema que resuelve:** Clasificación y segmentación de datos por categorías o zonas geográficas. Representa una nueva familia: **Agrupamiento / Clasificación**.

### 3. Ficha: `partition` (Familia: Dividir una lista)
1. **Qué hace:** Divide la colección en una tupla de dos listas `(List[A], List[A])` evaluando un predicado booleano en una sola pasada.
2. **Firma y lambda:** Recibe `p: A => Boolean`. Devuelve una tupla `(cumplen, noCumplen)`.
3. **Ejemplo en Scala:**
   ```scala
   val (normales, criticos) = catalogo.partition(p => p.stock >= 5)
   ```
   *Salida:* `Tupla con 8 elementos en _1 y 2 elementos en _2.`
4. **Problema que resuelve:** Separación de lotes en procesamiento de datos (por ejemplo, pagos aprobados vs rechazados). Se deriva de la **Selección**.

### 4. Ficha: `find` (Familia: Buscar)
1. **Qué hace:** Retorna el primer elemento que cumpla una condición booleana encapsulado en un `Option[A]`, deteniendo la búsqueda de inmediato.
2. **Firma y lambda:** Recibe `p: A => Boolean`. Retorna `Option[A]` (`Some(valor)` o `None`).
3. **Ejemplo en Scala:**
   ```scala
   val buscado = catalogo.find(p => p.categoria == "Suspension")
   ```
   *Salida:* `Some(Producto(Amortiguador, Suspension, 65.0, 4))`
4. **Problema que resuelve:** Búsquedas puntuales rápidas sin recorrer la colección completa y evitando excepciones de puntero nulo. Se relaciona con **Validación**.

### 4.3 Combinación de operaciones
* **Pregunta formulada:** *"¿Cuáles son los productos de la categoría 'Motor' que cuentan con más de 10 unidades en stock, ordenados de forma descendente por precio?"*
* **Cadena de operaciones:** `groupBy` (nueva) $\rightarrow$ `filter` (vista en clase) $\rightarrow$ `sortBy` (nueva)
  ```scala
  val resultado = catalogo
    .groupBy(p => p.categoria)("Motor") // 1. Operación nueva: agrupa y extrae 'Motor'
    .filter(p => p.stock > 10)          // 2. Operación de clase: filtra stock > 10
    .sortBy(p => -p.precio)             // 3. Operación nueva: ordena descendente
  ```
* **Explicación de cada paso:**
  1. `groupBy(p => p.categoria)("Motor")`: Agrupa todo el inventario y selecciona la sublista correspondiente a la clave `"Motor"`.
  2. `filter(p => p.stock > 10)`: Descarta ítems con bajo inventario, conservando solo aquellos con stock mayor a 10.
  3. `sortBy(p => -p.precio)`: Aplica inversión de signo al precio para ordenar la lista resultante de mayor a menor.
* **Salida de la cadena:**
  ```text
  - Filtro de Aire (Precio: $12.0 | Stock: 20)
  - Bujia Iridium (Precio: $10.0 | Stock: 40)
  - Filtro de Aceite (Precio: $8.0 | Stock: 30)
  ```

---

## Parte 5: Comparar y Concluir

### 5.1 Tabla comparativa

| Criterio | Scala | Java (Streams) | Python |
|:---|:---|:---|:---|
| **Sintaxis de la lambda** | Concisa: `p => p.stock < 5` o `_.stock < 5` | Flecha: `p -> p.stock() < 5` | Expresión: `lambda p: p.stock < 5` |
| **Cómo se declara el tipo** | Inferencia estática robusta; no exige declarar tipos en argumentos de la lambda. | Inferencia contextual estática mediante interfaces funcionales (`Predicate`, `Function`). | Tipado dinámico implícito en tiempo de ejecución. |
| **Operaciones con listas disponibles** | Nativas en la colección `List` mediante métodos de instancia fluidos. | Requiere encapsular en `.stream()`, encadenar y recolectar con `.toList()`. | Funciones globales (`filter`, `map`) o *list comprehensions*. |
| **Restricción del lenguaje** | Sin restricciones notorias; permite bloques multilínea y *pattern matching*. | Las variables capturadas en closures deben ser *efectivamente finales* (`effectively final`). | `lambda` está restringida a una sola expresión (no admite `return`, `for` o `try`). |
| **Legibilidad (opinión)** | **Excelente:** Código declarativo limpio, sin código boilerplate. | **Media:** Bastante código accesorio debido a llamadas a `.stream()` y wrappers. | **Buena en expresiones simples**, pero incómoda en agregaciones que requieren importar módulos. |

* **¿En qué lenguaje fue más natural resolver los problemas?**  
  En **Scala**, porque sus colecciones ya incorporan todas las operaciones de orden superior sin necesidad de convertir la lista a un flujo intermedio (como exige Java) ni requerir librerías adicionales para acumulaciones (como `functools` en Python).
* **¿En qué tipo de problema encontraste más diferencias entre lenguajes y por qué?**  
  En la **Acumulación (`foldLeft` / `reduce`)**. En Scala se resuelve directamente como `catalogo.foldLeft(0.0)((acc, p) => ...)`. En Java, el método `.reduce()` obliga a manejar identity, accumulator y combiner para tipos heterogéneos. En Python, `reduce` no es nativo global y requiere importar el módulo `functools`.

### 5.2 Conclusión
Las funciones anónimas son esenciales para el procesamiento moderno de colecciones, ya que separan la mecánica del recorrido interno de los datos respecto a la lógica de negocio que se aplica a cada elemento. Esto produce un código inmutable, conciso y menos propenso a errores de estado compartido.

Esta característica es la base de tecnologías del mundo real como **Apache Spark**, donde las funciones anónimas permiten definir transformaciones sobre colecciones distribuidas (RDDs) que el motor planifica y ejecuta en paralelo a través de múltiples nodos de red sin requerir mecanismos de sincronización manuales.

---

## Instrucciones para ejecutar el código

1. **Scala (Parte 3):**
   ```bash
   cd scala
   scala-cli run Soluciones.scala
   ```
2. **Java (Parte 3):**
   ```bash
   cd java
   javac Soluciones.java
   java Soluciones
   ```
3. **Python (Parte 3):**
   ```bash
   cd python
   python3 soluciones.py
   ```
4. **Operaciones Nuevas en Scala (Parte 4):**
   ```bash
   cd operaciones
   scala-cli run OperacionesNuevas.scala
   ```

---

## Referencias
* Apache Spark Documentation. *RDD Programming Guide*. https://spark.apache.org/docs/latest/rdd-programming-guide.html
* Oracle. *Java Tutorials: Lambda Expressions*. https://docs.oracle.com/javase/tutorial/java/javaOO/lambdaexpressions.html
* Python Documentation. *Functional Programming HOWTO*. https://docs.python.org/3/howto/functional.html
* Scala Documentation. *Collections API*. https://docs.scala-lang.org/overviews/collections-2.13/overview.html
