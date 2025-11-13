#!/usr/bin/env python3
"""
Autoparts ETL Pipeline
Sistema de transformación y carga para gestión de repuestos automotrices

Autor: Fernando P. - Database Architect
Versión: 2.0
"""

import pandas as pd
import psycopg2
from datetime import datetime, timedelta
import logging
import json
import sys
import os

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('autoparts_etl.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

class AutopartsETL:
    def __init__(self, db_config):
        self.db_config = db_config
        self.connection = None
        self.logger = logging.getLogger(__name__)
        
    def connect_database(self):
        """Establece conexión con PostgreSQL"""
        try:
            self.connection = psycopg2.connect(**self.db_config)
            self.logger.info("✅ Conexión a PostgreSQL establecida")
            return True
        except Exception as e:
            self.logger.error(f"❌ Error conectando a la base de datos: {e}")
            return False
    
    def extract_inventory_data(self, source_type='sample'):
        """
        Extrae datos de inventario desde fuente especificada
        Returns: DataFrame con datos de productos e inventario
        """
        self.logger.info(f"📂 Extrayendo datos de inventario desde: {source_type}")
        
        try:
            if source_type == 'sample':
                # Datos de ejemplo de repuestos automotrices
                sample_data = {
                    'sku': ['AP001', 'AP002', 'AP003', 'AP004', 'AP005', 'AP006'],
                    'nombre': [
                        'Filtro Aceite Toyota Corolla',
                        'Pastillas Freno Delanteras Nissan',
                        'Batería 12V 60Ah Universal', 
                        'Aceite Motor 5W30 Sintético',
                        'Amortiguadores Delanteros VW',
                        'Bujías Iridio Standard'
                    ],
                    'numero_parte': ['FT-123', 'PB-456', 'BAT-789', 'OIL-012', 'AMT-345', 'BJI-678'],
                    'categoria': ['Filtros', 'Frenos', 'Eléctrico', 'Lubricantes', 'Suspensión', 'Motor'],
                    'precio_compra': [8.50, 45.00, 120.00, 15.75, 85.00, 12.50],
                    'precio_venta': [12.00, 65.00, 180.00, 22.50, 125.00, 18.75],
                    'proveedor': ['AutoParts Supply', 'Brake Systems MX', 'MotorTech', 'AutoParts Supply', 'MotorTech', 'AutoParts Supply'],
                    'stock_actual': [25, 12, 8, 45, 15, 30],
                    'stock_minimo': [10, 5, 3, 15, 8, 12],
                    'ubicacion': ['A1-02', 'B2-15', 'C3-08', 'A1-10', 'B1-05', 'A2-12']
                }
                
                df = pd.DataFrame(sample_data)
                self.logger.info(f"📊 Datos de ejemplo generados: {len(df)} productos")
                return df
                
            elif source_type == 'csv':
                # Cargar desde archivo CSV
                if os.path.exists('data/inventario.csv'):
                    df = pd.read_csv('data/inventario.csv')
                    self.logger.info(f"📊 Datos cargados desde CSV: {len(df)} productos")
                    return df
                else:
                    self.logger.warning("⚠️  Archivo CSV no encontrado, generando datos de ejemplo")
                    return self.extract_inventory_data('sample')
                    
            else:
                self.logger.error("❌ Tipo de fuente no soportado")
                return None
                
        except Exception as e:
            self.logger.error(f"❌ Error extrayendo datos: {e}")
            return None
    
    def transform_product_data(self, raw_df):
        """
        Transforma datos a formato estándar de la base de datos
        """
        self.logger.info("🔄 Transformando datos de productos")
        
        try:
            transformed_df = raw_df.copy()
            
            # Validar SKUs únicos
            if transformed_df['sku'].duplicated().any():
                self.logger.warning("⚠️  SKUs duplicados detectados, eliminando duplicados")
                transformed_df = transformed_df.drop_duplicates(subset=['sku'], keep='first')
            
            # Validar datos requeridos
            required_columns = ['sku', 'nombre', 'precio_compra', 'precio_venta', 'stock_actual', 'stock_minimo']
            missing_columns = [col for col in required_columns if col not in transformed_df.columns]
            if missing_columns:
                raise ValueError(f"Columnas requeridas faltantes: {missing_columns}")
            
            # Calcular margen de ganancia
            transformed_df['margen_ganancia'] = (
                (transformed_df['precio_venta'] - transformed_df['precio_compra']) / transformed_df['precio_compra'] * 100
            ).round(2)
            
            # Categorizar nivel de stock
            def categorizar_stock(row):
                if row['stock_actual'] == 0:
                    return 'SIN_STOCK'
                elif row['stock_actual'] <= row['stock_minimo']:
                    return 'CRÍTICO'
                elif row['stock_actual'] <= row['stock_minimo'] * 2:
                    return 'BAJO'
                else:
                    return 'NORMAL'
            
            transformed_df['nivel_stock'] = transformed_df.apply(categorizar_stock, axis=1)
            
            # Validar precios
            precios_invalidos = transformed_df[transformed_df['precio_venta'] <= transformed_df['precio_compra']]
            if not precios_invalidos.empty:
                self.logger.warning(f"⚠️  {len(precios_invalidos)} productos con precio de venta <= precio de compra")
            
            self.logger.info("✅ Transformación de datos completada")
            return transformed_df
            
        except Exception as e:
            self.logger.error(f"❌ Error transformando datos: {e}")
            return None
    
    def load_to_database(self, transformed_df):
        """
        Carga datos transformados a PostgreSQL
        """
        if not self.connection:
            self.logger.error("❌ No hay conexión a la base de datos")
            return False
        
        try:
            cursor = self.connection.cursor()
            
            # Obtener IDs de categorías y proveedores existentes
            cursor.execute("SELECT id, nombre FROM categorias")
            categorias_map = {nombre: id for id, nombre in cursor.fetchall()}
            
            cursor.execute("SELECT id, nombre FROM proveedores")
            proveedores_map = {nombre: id for id, nombre in cursor.fetchall()}
            
            productos_procesados = 0
            productos_actualizados = 0
            
            for _, row in transformed_df.iterrows():
                # Obtener categoria_id y proveedor_id
                categoria_id = categorias_map.get(row['categoria'])
                proveedor_id = proveedores_map.get(row['proveedor'])
                
                if not categoria_id:
                    self.logger.warning(f"⚠️  Categoría '{row['categoria']}' no encontrada, usando categoría por defecto")
                    categoria_id = 1  # ID por defecto
                
                if not proveedor_id:
                    self.logger.warning(f"⚠️  Proveedor '{row['proveedor']}' no encontrado, usando proveedor por defecto")
                    proveedor_id = 1  # ID por defecto
                
                # Insertar o actualizar producto
                insert_producto = """
                INSERT INTO productos (sku, nombre, numero_parte, categoria_id, proveedor_id, 
                                     precio_compra, precio_venta, garantia_meses)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (sku) DO UPDATE SET
                    nombre = EXCLUDED.nombre,
                    precio_compra = EXCLUDED.precio_compra,
                    precio_venta = EXCLUDED.precio_venta,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING sku
                """
                
                cursor.execute(insert_producto, (
                    row['sku'], row['nombre'], row.get('numero_parte', ''), 
                    categoria_id, proveedor_id, row['precio_compra'], 
                    row['precio_venta'], 12  # 12 meses de garantía por defecto
                ))
                
                resultado = cursor.fetchone()
                if resultado:
                    if cursor.rowcount == 1:  # Nuevo producto
                        productos_procesados += 1
                    else:  # Producto actualizado
                        productos_actualizados += 1
                
                # Insertar o actualizar inventario
                insert_inventario = """
                INSERT INTO inventario (producto_sku, stock_actual, stock_minimo, stock_maximo, ubicacion)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (producto_sku) DO UPDATE SET
                    stock_actual = EXCLUDED.stock_actual,
                    stock_minimo = EXCLUDED.stock_minimo,
                    ubicacion = EXCLUDED.ubicacion,
                    updated_at = CURRENT_TIMESTAMP
                """
                cursor.execute(insert_inventario, (
                    row['sku'], row['stock_actual'], row['stock_minimo'],
                    row.get('stock_maximo', row['stock_minimo'] * 4),  # Stock máximo calculado
                    row['ubicacion']
                ))
            
            self.connection.commit()
            cursor.close()
            
            self.logger.info(f"✅ Datos cargados: {productos_procesados} nuevos, {productos_actualizados} actualizados")
            
            # Resumen de niveles de stock
            niveles_stock = transformed_df['nivel_stock'].value_counts()
            for nivel, cantidad in niveles_stock.items():
                self.logger.info(f"   📦 {nivel}: {cantidad} productos")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error cargando datos: {e}")
            self.connection.rollback()
            return False
    
    def generate_stock_report(self):
        """
        Genera reporte de stock crítico desde la base de datos
        """
        if not self.connection:
            return None
        
        try:
            cursor = self.connection.cursor()
            
            query = """
            SELECT p.sku, p.nombre, i.stock_actual, i.stock_minimo, i.ubicacion,
                   CASE 
                       WHEN i.stock_actual = 0 THEN 'SIN_STOCK'
                       WHEN i.stock_actual <= i.stock_minimo THEN 'CRÍTICO'
                       ELSE 'NORMAL'
                   END as estado
            FROM productos p
            JOIN inventario i ON p.sku = i.producto_sku
            WHERE i.stock_actual <= i.stock_minimo AND p.activo = true
            ORDER BY i.stock_actual ASC, p.nombre
            """
            
            cursor.execute(query)
            resultados = cursor.fetchall()
            cursor.close()
            
            return resultados
            
        except Exception as e:
            self.logger.error(f"❌ Error generando reporte: {e}")
            return None
    
    def run_etl_pipeline(self, source_type='sample'):
        """
        Ejecuta el pipeline ETL completo
        """
        self.logger.info("🚀 Iniciando pipeline ETL de Autoparts")
        self.logger.info("=" * 50)
        
        start_time = datetime.now()
        
        # Conectar a la base de datos
        if not self.connect_database():
            return False
        
        try:
            # Extracción
            self.logger.info("📥 FASE 1: EXTRACCIÓN")
            raw_data = self.extract_inventory_data(source_type)
            if raw_data is None:
                return False
            
            # Transformación
            self.logger.info("🔄 FASE 2: TRANSFORMACIÓN")
            transformed_data = self.transform_product_data(raw_data)
            if transformed_data is None:
                return False
            
            # Carga
            self.logger.info("📤 FASE 3: CARGA")
            success = self.load_to_database(transformed_data)
            
            if success:
                # Generar reporte final
                self.logger.info("📊 FASE 4: REPORTING")
                reporte = self.generate_stock_report()
                
                tiempo_ejecucion = datetime.now() - start_time
                
                self.logger.info("=" * 50)
                self.logger.info("🎉 PIPELINE ETL COMPLETADO EXITOSAMENTE")
                self.logger.info(f"⏱️  Tiempo total: {tiempo_ejecucion.total_seconds():.2f} segundos")
                
                if reporte:
                    self.logger.info(f"📋 Productos con stock crítico: {len(reporte)}")
                    if len(reporte) > 0:
                        self.logger.info("🔴 Productos que requieren atención inmediata:")
                        for item in reporte[:5]:  # Mostrar solo los primeros 5
                            self.logger.info(f"   ⚠️  {item[0]} - {item[1]}: {item[2]}/{item[3]} ({item[4]})")
                        if len(reporte) > 5:
                            self.logger.info(f"   ... y {len(reporte) - 5} productos más")
                else:
                    self.logger.info("✅ No hay productos con stock crítico")
                    
            else:
                self.logger.error("💥 Pipeline ETL falló en la fase de carga")
                
            return success
            
        except Exception as e:
            self.logger.error(f"💥 Error crítico en pipeline ETL: {e}")
            return False
        finally:
            if self.connection:
                self.connection.close()
                self.logger.info("🔌 Conexión a base de datos cerrada")

def main():
    """Función principal"""
    
    # Configuración de la base de datos - AJUSTAR SEGÚN TU ENTORNO
    db_config = {
        'host': 'localhost',
        'database': 'autoparts_db',
        'user': 'postgres',
        'password': 'password',
        'port': 5432
    }
    
    # Crear instancia del ETL
    etl = AutopartsETL(db_config)
    
    # Ejecutar pipeline (puedes cambiar 'sample' por 'csv' para cargar desde archivo)
    success = etl.run_etl_pipeline('sample')
    
    # Resultado final
    print("\n" + "=" * 60)
    if success:
        print("✅ AUTOPARTS ETL COMPLETADO EXITOSAMENTE")
        print("   Los datos de inventario han sido procesados y cargados")
        print("   Revisa el archivo autoparts_etl.log para detalles completos")
    else:
        print("❌ AUTOPARTS ETL FALLÓ")
        print("   Revisa el archivo autoparts_etl.log para identificar el error")
    print("=" * 60)

if __name__ == "__main__":
    main()
