package Practica

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions._

object examen {

  /** Ejercicio 1: Crear un DataFrame y realizar operaciones básicas
   * Pregunta: Crea un DataFrame a partir de una secuencia de tuplas que contenga información sobre
   * estudiantes (nombre, edad, calificación).
   * Realiza las siguientes operaciones:
   *
   * Muestra el esquema del DataFrame.
   * Filtra los estudiantes con una calificación mayor a 8.
   * Selecciona los nombres de los estudiantes y ordénalos por calificación de forma descendente.
   */
  val spark: SparkSession = SparkSession.builder().appName("SparkTest").master("local[*]").config("spark.driver.memory", "4g").config("spark.executor.memory", "4g").config("spark.blockManager.port", "0").config("spark.memory.fraction", "0.8").config("spark.storage.memoryFraction", "0.5").config("spark.driver.host", "127.0.0.1").getOrCreate()
  val sc: SparkContext = spark.sparkContext
  sc.setLogLevel("ERROR")

  import spark.implicits._

  val estudiantes = Seq(
    ("Pablo", 34, 9.1), ("Lucía", 28, 8.7), ("Carlos", 22, 7.9),
    ("Mariana", 31, 9.5), ("Sofía", 26, 8.2), ("Alejandro", 30, 7.4),
    ("Valeria", 24, 9.3), ("Fernando", 27, 8.1), ("Gabriela", 29, 9.0),
    ("Andrés", 32, 8.8)
  )
  // Crear el DataFrame a partir de la variable alumnos
  val df = sc.parallelize(estudiantes).toDF("nombre", "edad", "calificacion")

  def ejercicio1(df: DataFrame)(spark: SparkSession): Unit = {
    // 1. Mostrar el esquema del DataFrame
    println("Esquema del DataFrame:")
    df.printSchema()

    // 2. Filtrar estudiantes con calificación mayor a 8
    val dfFiltrado = df.filter($"calificacion" > 8)

    println("Estudiantes con calificación mayor a 8:")
    dfFiltrado.show()

    // 3. Seleccionar nombres y ordenarlos por calificación descendente
    val dfOrdenado = dfFiltrado.select("nombre", "calificacion").orderBy($"calificacion".desc)

    println("Nombres ordenados por calificación descendente:")
    dfOrdenado.show()
  }

  /** Ejercicio 2: UDF (User Defined Function)
   * Pregunta: Define una función que determine si un número es par o impar.
   * Aplica esta función a una columna de un DataFrame que contenga una lista de números.
   */

  // Crear un DataFrame de ejemplo con una lista de números
  def ejercicio2(numeros: DataFrame)(spark: SparkSession): DataFrame = {
    import spark.implicits._

    // Definir una UDF para determinar si un número es par o impar
    val esParImpar = udf((numero: Int) => if (numero % 2 == 0) "Par" else "Impar")

    // Agregar una nueva columna al DataFrame con el resultado de la UDF
    val numerosConParidad = numeros.withColumn("paridad", esParImpar($"numero"))

    // Retornar el DataFrame modificado
    numerosConParidad
  }

  /** Ejercicio 3: Joins y agregaciones
   * Pregunta: Dado dos DataFrames,
   * uno con información de estudiantes (id, nombre)
   * y otro con calificaciones (id_estudiante, asignatura, calificacion),
   * realiza un join entre ellos y calcula el promedio de calificaciones por estudiante.
   */

  def ejercicio3(estudiantes: DataFrame, calificaciones: DataFrame): DataFrame = {
    import spark.implicits._

    // Realizar el join entre estudiantes y calificaciones
    val joinedDF = estudiantes
      .join(calificaciones, estudiantes("id") === calificaciones("id_estudiante"))
      .select("id", "nombre", "asignatura", "calificacion")

    // Calcular el promedio de calificaciones por estudiante
    val promedioDF = joinedDF
      .groupBy("id", "nombre")
      .agg(avg("calificacion").alias("promedio_calificacion"))

    // Retornar el DataFrame con el promedio
    promedioDF
  }

  /**Ejercicio 4: Uso de RDDs:
   *Pregunta: Crea un RDD a partir de una lista de palabras y cuenta la cantidad de ocurrencias de cada palabra.
   */

  def ejercicio4(palabras: List[String])(spark :SparkSession): RDD[(String, Int)] = {
    // Crear un RDD a partir de la lista de palabras
    val palabrasRDD: RDD[String] = sc.parallelize(palabras)

    // Transformar el RDD para contar las ocurrencias de cada palabra
    val conteoPalabras: RDD[(String, Int)] = palabrasRDD
      .map(palabra => (palabra, 1)) // Crear un par clave-valor (palabra, 1)
      .reduceByKey(_ + _) // Sumar los valores por cada clave (palabra)

    // Retornar el RDD con los conteos
    conteoPalabras
  }
  /**Ejercicio 5: Procesamiento de archivos
  *Pregunta: Carga un archivo CSV que contenga información sobre
            *ventas (id_venta, id_producto, cantidad, precio_unitario)
            *y calcula el ingreso total (cantidad * precio_unitario) por producto.
  */
  def ejercicio5(ventas: DataFrame)(spark:SparkSession): DataFrame = {

      // Calcular el ingreso total (cantidad * precio_unitario) por producto
      val ingresosPorProducto = ventas
        .withColumn("ingreso_total", $"cantidad" * $"precio_unitario") // Calcular la columna ingreso_total
        .groupBy("id_producto") // Agrupar por id_producto
        .agg(sum("ingreso_total").alias("ingreso_total")) // Sumar ingresos por producto

      // Retornar el DataFrame con el ingreso total por producto
      ingresosPorProducto
    }
}

