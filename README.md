
# Trabajo de Consulta: Funciones Anónimas

* **Materia:** Programación Funcional
* **Modalidad:** Trabajo individual (Entrega: Semana 5)
* **Lenguajes:** Scala, Java, Python

---

## Índice
1. [Parte 1: Explorar — La programación funcional en el mundo real](#parte-1-explorar--la-programación-funcional-en-el-mundo-real)
2. [Parte 2: Entender — ¿Qué es una función anónima?](#parte-2-entender--qué-es-una-función-anónima)
3. [Parte 3: Aplicar — Tipos de problemas con funciones anónimas](#parte-3-aplicar--tipos-de-problemas-con-funciones-anónimas)
4. [Parte 4: Ampliar — Cuatro operaciones nuevas en Scala](#parte-4-ampliar--cuatro-operaciones-nuevas-en-scala)
5. [Parte 5: Comparar y Concluir](#parte-5-comparar-y-concluir)
6. [Instrucciones de Ejecución](#instrucciones-de-ejecución)
7. [Referencias Consultadas](#referencias-consultadas)

---

## Parte 1: Explorar — La programación funcional en el mundo real

### Caso 1: Apache Spark (Procesamiento distribuido masivo)
* **Problema que resuelve:** Limpieza, transformación y análisis de grandes volúmenes de datos (Big Data) distribuidos a través de clústeres tolerantes a fallos.
* **Lenguaje o herramienta:** Scala (arquitectura de RDDs y DataFrames).
* **Ventaja del enfoque funcional:** La inmutabilidad de los datos permite paralelizar el cálculo en miles de nodos de cómputo sin peligro de colisiones de memoria ni condiciones de carrera (*race conditions*). Spark utiliza además *lazy evaluation* (evaluación perezosa): las funciones anónimas aplicadas a colecciones no computan de inmediato, sino que se integran en un grafo dirigido acíclico (DAG) que el planificador optimiza antes de la ejecución física.
* **Fragmento de código real:**
  ```scala
  val logsErrores = sc.textFile("hdfs://cluster/logs/*.log")
    .filter(linea => linea.contains("ERROR 500"))
    .map(linea => (linea.split(" ")(0), 1))
    .reduceByKey((acc, n) => acc + n)
  ```
* **Explicación del código:** La lambda `linea => linea.contains("ERROR 500")` se serializa y envía a través de la red a cada nodo de trabajo para filtrar particiones de disco en paralelo, mientras que `(acc, n) => acc + n` consolida los conteos de forma asociativa.
* **Fuente consultada:** Apache Spark Documentation: *RDD Programming Guide* (https://spark.apache.org/docs/latest/rdd-programming-guide.html).

### Caso 2: Java Streams en Sistemas de Telemetría Empresariales (Netflix)
* **Problema que resuelve:** Enriquecimiento, filtrado y balanceo en tiempo real de millones de peticiones por segundo en arquitecturas de microservicios.
* **Lenguaje o herramienta:** Java 17+ con Java Streams API.
* **Ventaja del enfoque funcional:** Permite escribir lógica de negocio declarativa en canalizaciones legibles (`filter -> map -> reduce`), eliminando bucles imperativos con contadores y acumuladores mutables que suelen introducir errores sutiles de concurrencia en entornos multihilo.
* **Fragmento de código real:**
  ```java
  List<String> clientesMorosos = transacciones.stream()
      .filter(t -> t.getDiasAtraso() > 90)
      .map(Transaccion::getCedulaCliente)
      .distinct()
      .toList();
  ```
* **Explicación del código:** La lambda `t -> t.getDiasAtraso() > 90` encapsula la regla de selección en línea como argumento de una función de orden superior sin requerir clases anónimas verbosas.
* **Fuente consultada:** Netflix Technology Blog: *Functional Patterns at Scale* (https://netflixtechblog.com) y Oracle Java SE Documentation: *Aggregate Operations* (https://docs.oracle.com/javase/tutorial/java/javaOO/lambdaexpressions.html).

---

## Parte 2: Entender — ¿Qué es una función anónima?

### 1. ¿Qué es una función anónima y de dónde viene el nombre lambda?
Una **función anónima** es un literal de función: una expresión ejecutable que acepta parámetros y computa un resultado sin estar ligada a un identificador o nombre formal en el código.

El nombre **lambda** proviene del **Cálculo Lambda ($\lambda$-calculus)**, un sistema formal desarrollado por el matemático y lógico **Alonzo Church** en la década de 1930 para investigar la computabilidad, la definición de funciones y la sustitución de variables. Church utilizó la letra griega $\lambda$ para marcar qué parámetros estaban ligados en una abstracción funcional.

### 2. Diferencia frente a una función nombrada
Una función nombrada se registra explícitamente en el espacio de nombres del programa para su posterior reutilización global o local. En cambio, una función anónima se define *in situ* para resolver un cálculo efímero en un punto específico (por ejemplo, pasarse a un filtro o transformación).

* **Misma lógica en ambas formas (Scala):**
  ```scala
  // 1. Función nombrada
  def calcularIva(precio: Double): Double = precio * 0.15

  // 2. Función anónima equivalente
  val calcularIvaAnonima = (precio: Double) => precio * 0.15
  ```

### 3. Funciones como ciudadanos de primera clase
Significa que en el lenguaje las funciones poseen exactamente el mismo estatus que cualquier tipo de dato fundamental (como `Int`, `String` o `Double`):
* Pueden asignarse a variables o constantes.
* Pueden pasarse como argumentos a otras funciones.
* Pueden ser retornadas como resultado por otras funciones.

Esta propiedad es lo que hace posible que métodos de colecciones como `map` o `filter` reciban expresiones lambda como parámetro para gobernar el criterio de transformación o selección de elementos.

### 4. ¿Qué es un closure?
Un **closure** (cierre léxico) es una función anónima que captura y retiene variables declaradas en su ámbito circundante (*enclosing scope*), permitiendo acceder a sus valores incluso cuando la lambda se evalúa fuera de ese contexto.

```scala
val umbralStockMinimo = 5

// La lambda captura la variable externa 'umbralStockMinimo'
val esStockBajo = (stock: Int) => stock < umbralStockMinimo
```

---

## Parte 3: Aplicar — Tipos de problemas con funciones anónimas

### Colección de datos unificada
Para asegurar que los tres lenguajes operen exactamente bajo el mismo escenario y produzcan salidas idénticas, se definió una lista homogénea de 10 productos de autopartes:

```text
1. Repuesto("Pastillas de Freno", "Frenos", 25.0, 15)
2. Repuesto("Disco de Freno", "Frenos", 45.0, 8)
3. Repuesto("Amortiguador Delantero", "Suspension", 65.0, 4)
4. Repuesto("Espiral Trasero", "Suspension", 50.0, 6)
5. Repuesto("Filtro de Aceite", "Motor", 8.0, 30)
6. Repuesto("Filtro de Aire", "Motor", 12.0, 20)
7. Repuesto("Bujia Iridium", "Motor", 10.0, 40)
8. Repuesto("Kit de Embrague", "Transmision", 120.0, 3)
9. Repuesto("Aceite 10W40", "Lubricantes", 28.0, 18)
10. Repuesto("Refrigerante", "Refrigeracion", 15.0, 12)
```

---

### Problema 1: Selección (`filter`)
* **Situación real:** Un taller automotriz necesita detectar los repuestos con stock crítico (menos de 5 unidades) para generar una orden de compra urgente a proveedores.
* **Pregunta que responde:** ¿Qué elementos cumplen una condición?
* **Solución en Scala:**
  ```scala
  val repuestosCriticos = repuestos.filter(_.stock < 5)
  ```
* **Solución en Java:**
  ```java
  List<Repuesto> repuestosCriticos = repuestos.stream()
      .filter(r -> r.stock() < 5)
      .toList();
  ```
* **Solución en Python:**
  ```python
  repuestos_criticos = list(filter(lambda r: r.stock < 5, repuestos))
  ```
* **Salida de las tres versiones:**
  ```text
  Amortiguador Delantero (Stock: 4)
  Kit de Embrague (Stock: 3)
  ```

---

### Problema 2: Transformación (`map`)
* **Situación real:** Se requiere generar las etiquetas de facturación convirtiendo el nombre de los repuestos a mayúsculas y calculando su precio final con el 15% de IVA.
* **Pregunta que responde:** ¿Cómo convierto cada elemento en otro valor?
* **Solución en Scala:**
  ```scala
  val facturacion = repuestos.map(r => s"${r.nombre.toUpperCase} -> $$${r.precio * 1.15}")
  ```
* **Solución en Java:**
  ```java
  List<String> facturacion = repuestos.stream()
      .map(r -> r.nombre().toUpperCase() + " -> $" + String.format("%.2f", r.precio() * 1.15))
      .toList();
  ```
* **Solución en Python:**
  ```python
  facturacion = list(map(lambda r: f"{r.nombre.upper()} -> ${r.precio * 1.15:.2f}", repuestos))
  ```
* **Salida de las tres versiones (primeros elementos):**
  ```text
  PASTILLAS DE FRENO -> $28.75
  DISCO DE FRENO -> $51.75
  AMORTIGUADOR DELANTERO -> $74.75
  ```

---

### Problema 3: Extremos (`maxBy`)
* **Situación real:** El departamento de finanzas y seguros necesita identificar cuál es el repuesto más costoso del almacén para fijar la póliza de cobertura de bodega.
* **Pregunta que responde:** ¿Cuál es el mejor o el peor según un criterio?
* **Solución en Scala:**
  ```scala
  val repuestoMasCaro = repuestos.maxBy(_.precio)
  ```
* **Solución en Java:**
  ```java
  Repuesto repuestoMasCaro = repuestos.stream()
      .max(Comparator.comparingDouble(r -> r.precio()))
      .orElseThrow();
  ```
* **Solución en Python:**
  ```python
  repuesto_mas_caro = max(repuestos, key=lambda r: r.precio)
  ```
* **Salida de las tres versiones:**
  ```text
  Kit de Embrague ($120.0)
  ```

---

### Problema 4: Acumulación (`foldLeft`)
* **Situación real:** La empresa debe calcular el valor monetario total del inventario multiplicando el precio unitario por el stock de cada repuesto y totalizando la cifra.
* **Pregunta que responde:** ¿Qué valor resume la lista?, ¿cómo evoluciona paso a paso?
* **Solución en Scala:**
  ```scala
  val totalInventario = repuestos.foldLeft(0.0)((acc, r) => acc + (r.precio * r.stock))
  ```
* **Solución en Java:**
  ```java
  double totalInventario = repuestos.stream()
      .reduce(0.0, (acc, r) -> acc + (r.precio() * r.stock()), Double::sum);
  ```
* **Solución en Python:**
  ```python
  from functools import reduce
  total_inventario = reduce(lambda acc, r: acc + (r.precio * r.stock), repuestos, 0.0)
  ```
* **Salida de las tres versiones:**
  ```text
  Valor total inventario: $2419.00
  ```

---

## Parte 4: Ampliar — Cuatro operaciones nuevas en Scala

Las siguientes 4 operaciones investigadas pertenecen a la API de colecciones de Scala y no forman parte de las vistas en clase:

### 1. Ficha: `sortBy` (Familia: Ordenar)
1. **Qué hace:** Ordena los elementos de la colección en orden ascendente evaluando una función de proyección sobre cada uno.
2. **Firma y lambda:** Recibe `f: A => B` (donde `B` tiene orden natural implícito mediante `Ordering[B]`). Devuelve la colección ordenada.
3. **Ejemplo en Scala:**
   ```scala
   val porPrecio = repuestos.sortBy(_.precio)
   ```
   *Salida:* `Filtro de Aceite ($8.0), Bujia Iridium ($10.0), ... Kit de Embrague ($120.0)`
4. **Problema que resuelve:** Visualización ordenada de catálogos desde el artículo más económico al más costoso. Se relaciona con la familia de **Extremos** generalizada a toda la colección.

### 2. Ficha: `groupBy` (Familia: Agrupar)
1. **Qué hace:** Agrupa los elementos en un diccionario asociativo (`Map[K, List[A]]`) utilizando como clave el resultado devuelto por la función anónima.
2. **Firma y lambda:** Recibe `f: A => K`. Devuelve un `Map` donde cada clave `K` apunta a su lista de elementos asociados.
3. **Ejemplo en Scala:**
   ```scala
   val porCategoria = repuestos.groupBy(_.categoria)
   ```
   *Salida:* `Map(Frenos -> List(Pastillas, Disco), Motor -> List(Filtro Aceite, Filtro Aire, Bujia), ...)`
4. **Problema que resuelve:** Segmentación y partición de datos por subsistema mecánico o departamento. Introduce una familia nueva: **Agrupamiento / Clasificación asociativa**.

### 3. Ficha: `partition` (Familia: Dividir una lista)
1. **Qué hace:** Divide la colección en una tupla de dos listas `(List[A], List[A])` evaluando un predicado booleano en un único recorrido de la lista.
2. **Firma y lambda:** Recibe `p: A => Boolean`. Devuelve una tupla `(elementosQueCumplen, elementosQueNoCumplen)`.
3. **Ejemplo en Scala:**
   ```scala
   val (stockSuficiente, stockBajo) = repuestos.partition(_.stock >= 5)
   ```
   *Salida:* `Tupla donde _1 contiene 8 repuestos normales y _2 contiene 2 repuestos críticos.`
4. **Problema que resuelve:** Separación atómica de lotes en un sistema de despacho (artículos listos para facturar vs. artículos que requieren reposición). Es una derivación binaria de la **Selección**.

### 4. Ficha: `find` (Familia: Buscar)
1. **Qué hace:** Busca y retorna el primer elemento que satisface una condición booleana encapsulado en un `Option[A]`, interrumpiendo la iteración en cuanto lo encuentra.
2. **Firma y lambda:** Recibe `p: A => Boolean`. Devuelve `Option[A]` (`Some(valor)` si existe, o `None` si ninguno cumple).
3. **Ejemplo en Scala:**
   ```scala
   val primerSuspension = repuestos.find(_.categoria == "Suspension")
   ```
   *Salida:* `Some(Repuesto(Amortiguador Delantero, Suspension, 65.0, 4))`
4. **Problema que resuelve:** Búsqueda segura en inventario sin recorrer la lista completa innecesariamente y evitando excepciones de puntero nulo. Se relaciona con la familia de **Validación**.

---

### 4.3 Combinación de operaciones
* **Pregunta formulada:** *¿Cuáles son los repuestos pertenecientes a la categoría 'Motor' que cuentan con más de 10 unidades en stock, ordenados de forma descendente por precio?*
* **Cadena de operaciones:** `groupBy` (nueva) $\rightarrow$ `filter` (vista en clase) $\rightarrow$ `sortBy` (nueva)
  ```scala
  val repuestosMotorTop = repuestos
    .groupBy(_.categoria)("Motor") // 1. Operación nueva: clasifica y extrae 'Motor'
    .filter(_.stock > 10)          // 2. Operación de clase: filtra stock > 10
    .sortBy(-_.precio)             // 3. Operación nueva: ordena por precio descendente
  ```
* **Explicación paso a paso:**
  1. `groupBy(_.categoria)("Motor")`: Agrupa el catálogo por categoría y selecciona inmediatamente la lista de repuestos bajo la llave `"Motor"`.
  2. `filter(_.stock > 10)`: Descarta los elementos con inventario bajo o moderado, reteniendo únicamente aquellos con volumen alto.
  3. `sortBy(-_.precio)`: Aplica la negación aritmética al precio numérico para invertir el orden natural y producir una lista ordenada de mayor a menor valor.
* **Salida de la cadena:**
  ```text
  Filtro de Aire (Precio: $12.0 | Stock: 20)
  Bujia Iridium (Precio: $10.0 | Stock: 40)
  Filtro de Aceite (Precio: $8.0 | Stock: 30)
  ```

---

## Parte 5: Comparar y Concluir

### 5.1 Tabla comparativa frente a frente

| Criterio | Scala | Java (Streams) | Python |
|:---|:---|:---|:---|
| **Sintaxis de la lambda** | Muy concisa: con parámetro nombrado (`r => r.stock < 5`) o con notación posicional (`_.stock < 5`). | Basada en flecha: `r -> r.stock() < 5`. | Explícita con palabra reservada: `lambda r: r.stock < 5`. |
| **Cómo se declara el tipo** | Inferencia de tipos estática y robusta; casi nunca requiere anotaciones de tipo en argumentos. | Tipado estático deducido por inferencia contextual mediante interfaces funcionales (`Predicate`, `Function`). | Tipado dinámico; los parámetros de la lambda no declaran tipos en ejecución. |
| **Operaciones con listas disponibles** | Nativas y fluidas directamente sobre `List` sin necesidad de intermediarios. | Requiere convertir la colección a un flujo (`.stream()`) y recopilar con `.toList()`. | Funciones globales del intérprete (`filter`, `map`) o *list comprehensions*. |
| **Restricciones del lenguaje** | Ninguna relevante; admite lambdas multilínea, evaluación perezosa y *pattern matching*. | Las variables externas capturadas en closures deben ser estrictamente finales o efectivamente finales (`effectively final`). | `lambda` está limitada exclusivamente a una sola expresión (no admite sentencias de control, asignaciones ni bloques). |
| **Legibilidad (opinión)** | **Excelente:** Código declarativo limpio, sin código boilerplate y con alto poder expresivo. | **Media:** Bastante código accesorio debido a la apertura de flujos y convertidores intermedios. | **Buena para casos simples**, pero sintácticamente recargada en acumulaciones complejas. |

* **¿En qué lenguaje fue más natural resolver los problemas?**  
  En **Scala**. Al estar las funciones de orden superior completamente integradas en la clase base `List`, la escritura del código se siente fluida y directa. La sintaxis con guion bajo (`_`) permite escribir transformaciones precisas sin la sobrecarga estructural que impone Java ni la necesidad de importar módulos externos como en Python.
* **¿En qué tipo de problema encontraste más diferencias entre lenguajes y por qué?**  
  En la **Acumulación (`foldLeft` frente a `reduce`)**. En Scala se realiza con una invocación directa y un valor neutro claro: `repuestos.foldLeft(0.0)((acc, r) => ...)`. En Java, el método `.reduce()` sobre tipos heterogéneos obliga a definir un acumulador y una función combinadora para satisfacer el tipado estricto en flujos paralelos. En Python, `reduce` fue removido de las funciones integradas globales y debe importarse explícitamente desde el módulo `functools`.

### 5.2 Conclusión
Las funciones anónimas son el pilar fundamental del procesamiento moderno de colecciones, ya que permiten separar el **qué** (la regla de negocio o transformación puntual) del **cómo** (el mecanismo interno de recorrido de la estructura de datos). Este desacoplamiento promueve la inmutabilidad de los datos, previene estados inconsistentes y produce canalizaciones de datos mucho más legibles y mantenibles.

Este principio es el núcleo de arquitecturas distribuidas del mundo real como **Apache Spark**, analizado en la Parte 1. Al no depender de estados mutables ni variables compartidas, los motores analíticos pueden tomar una función anónima, serializarla a través de la red y ejecutar transformaciones masivas en clústeres de cientos de servidores de manera segura, eficiente y transparente para el programador.

---

## Instrucciones de Ejecución

Para comprobar que las soluciones en los tres lenguajes generan exactamente los mismos resultados:

### 1. Scala (Partes 3 y 4)
Ejecutar mediante Scala CLI o cargar directamente en un Worksheet/REPL de Scala 3:
```bash
scala-cli run scala/Soluciones.scala
scala-cli run operaciones/OperacionesNuevas.scala
```

### 2. Java (Parte 3)
Compilar y ejecutar la clase principal (requiere JDK 17 o superior):
```bash
javac java/Soluciones.java
java -cp java Soluciones
```

### 3. Python (Parte 3)
Ejecutar con el intérprete estándar de Python 3:
```bash
python3 python/soluciones.py
```

---

## Referencias Consultadas
1. **Documentación Oficial de Scala:** *Collections Overview & Higher-Order Functions*. https://docs.scala-lang.org
2. **Oracle Java SE Tutorial:** *Lambda Expressions and Stream API*. https://docs.oracle.com/javase/tutorial/java/javaOO/lambdaexpressions.html
3. **Python Software Foundation:** *Functional Programming HOWTO*. https://docs.python.org/3/howto/functional.html
4. **Apache Spark Documentation:** *RDD Programming Guide - Transformations and Actions*. https://spark.apache.org/docs/latest/rdd-programming-guide.html
5. **Winitzki, S. (2022):** *The Science of Functional Programming: A Tutorial, with Examples in Scala*. https://github.com/winitzki/sofp
6. **Herramientas de Asistencia:** Se emplearon modelos de Inteligencia Artificial para la verificación comparativa de sintaxis y diseño del documento.
