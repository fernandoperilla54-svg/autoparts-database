-- =============================================
-- AUTOPARTS DATABASE SCHEMA
-- Sistema de Gestión para Distribuidora de Repuestos Automotrices
-- Versión: 2.0
-- Autor: Fernando P. - Database Architect
-- =============================================

-- Creación de la base de datos (ejecutar primero si no existe)
-- CREATE DATABASE autoparts_db;

-- \c autoparts_db; -- Conectar a la base de datos

-- =============================================
-- TABLAS MAESTRAS
-- =============================================

CREATE TABLE IF NOT EXISTS proveedores (
    id SERIAL PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,
    contacto VARCHAR(100),
    telefono VARCHAR(20),
    email VARCHAR(100),
    direccion TEXT,
    rfc VARCHAR(13),
    terminos_pago VARCHAR(50) DEFAULT '30 días',
    lead_time_dias INTEGER DEFAULT 7,
    activo BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS categorias (
    id SERIAL PRIMARY KEY,
    nombre VARCHAR(50) NOT NULL UNIQUE,
    descripcion TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS marcas_vehiculos (
    id SERIAL PRIMARY KEY,
    nombre VARCHAR(50) NOT NULL UNIQUE,
    pais_origen VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================
-- TABLAS DE PRODUCTOS E INVENTARIO
-- =============================================

CREATE TABLE IF NOT EXISTS productos (
    sku VARCHAR(20) PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,
    numero_parte VARCHAR(50),
    descripcion TEXT,
    precio_compra DECIMAL(10,2) CHECK (precio_compra >= 0),
    precio_venta DECIMAL(10,2) CHECK (precio_venta >= 0),
    categoria_id INTEGER REFERENCES categorias(id),
    proveedor_id INTEGER REFERENCES proveedores(id),
    garantia_meses INTEGER DEFAULT 12,
    peso_kg DECIMAL(8,3),
    dimensiones JSONB,
    activo BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS inventario (
    producto_sku VARCHAR(20) PRIMARY KEY REFERENCES productos(sku) ON DELETE CASCADE,
    stock_actual INTEGER NOT NULL DEFAULT 0 CHECK (stock_actual >= 0),
    stock_minimo INTEGER NOT NULL DEFAULT 5 CHECK (stock_minimo >= 0),
    stock_maximo INTEGER CHECK (stock_maximo >= 0),
    ubicacion VARCHAR(50),
    ultimo_movimiento TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================
-- TABLAS DE CLIENTES Y VEHÍCULOS
-- =============================================

CREATE TABLE IF NOT EXISTS clientes (
    id SERIAL PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,
    tipo VARCHAR(20) NOT NULL CHECK (tipo IN ('taller', 'distribuidor', 'particular', 'empresa')),
    telefono VARCHAR(20),
    email VARCHAR(100),
    direccion TEXT,
    rfc VARCHAR(13),
    credito_limite DECIMAL(10,2) DEFAULT 0,
    dias_credito INTEGER DEFAULT 0,
    activo BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS vehiculos (
    id SERIAL PRIMARY KEY,
    marca_id INTEGER REFERENCES marcas_vehiculos(id),
    modelo VARCHAR(50) NOT NULL,
    año INTEGER CHECK (año >= 1900 AND año <= EXTRACT(YEAR FROM CURRENT_DATE) + 1),
    motor VARCHAR(50),
    transmision VARCHAR(20) CHECK (transmisión IN ('manual', 'automática', 'cvt')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS compatibilidades (
    producto_sku VARCHAR(20) REFERENCES productos(sku) ON DELETE CASCADE,
    vehiculo_id INTEGER REFERENCES vehiculos(id) ON DELETE CASCADE,
    notas TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (producto_sku, vehiculo_id)
);

-- =============================================
-- TABLAS DE OPERACIONES
-- =============================================

CREATE TABLE IF NOT EXISTS pedidos (
    id SERIAL PRIMARY KEY,
    cliente_id INTEGER REFERENCES clientes(id),
    fecha_pedido DATE DEFAULT CURRENT_DATE,
    fecha_entrega_estimada DATE,
    estado VARCHAR(20) DEFAULT 'pendiente' 
        CHECK (estado IN ('pendiente', 'confirmado', 'preparacion', 'en_camino', 'entregado', 'cancelado')),
    prioridad VARCHAR(10) DEFAULT 'normal' 
        CHECK (prioridad IN ('baja', 'normal', 'alta', 'urgente')),
    subtotal DECIMAL(10,2) DEFAULT 0,
    iva DECIMAL(10,2) DEFAULT 0,
    total DECIMAL(10,2) DEFAULT 0,
    notas TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS detalle_pedidos (
    id SERIAL PRIMARY KEY,
    pedido_id INTEGER REFERENCES pedidos(id) ON DELETE CASCADE,
    producto_sku VARCHAR(20) REFERENCES productos(sku),
    cantidad INTEGER NOT NULL CHECK (cantidad > 0),
    precio_unitario DECIMAL(10,2) CHECK (precio_unitario >= 0),
    subtotal_linea DECIMAL(10,2) CHECK (subtotal_linea >= 0),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ventas (
    id SERIAL PRIMARY KEY,
    pedido_id INTEGER UNIQUE REFERENCES pedidos(id),
    fecha_venta TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metodo_pago VARCHAR(20) NOT NULL 
        CHECK (metodo_pago IN ('efectivo', 'tarjeta_credito', 'tarjeta_debito', 'transferencia', 'credito')),
    referencia_pago VARCHAR(50),
    subtotal DECIMAL(10,2) CHECK (subtotal >= 0),
    iva DECIMAL(10,2) CHECK (iva >= 0),
    total DECIMAL(10,2) CHECK (total >= 0),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================
-- ÍNDICES PARA OPTIMIZACIÓN
-- =============================================

-- Índices para productos
CREATE INDEX idx_productos_sku ON productos(sku);
CREATE INDEX idx_productos_categoria ON productos(categoria_id);
CREATE INDEX idx_productos_proveedor ON productos(proveedor_id);
CREATE INDEX idx_productos_activo ON productos(activo) WHERE activo = true;

-- Índices para inventario
CREATE INDEX idx_inventario_stock ON inventario(stock_actual);
CREATE INDEX idx_inventario_stock_minimo ON inventario((stock_actual - stock_minimo)) WHERE stock_actual <= stock_minimo;

-- Índices para clientes
CREATE INDEX idx_clientes_tipo ON clientes(tipo);
CREATE INDEX idx_clientes_activo ON clientes(activo) WHERE activo = true;

-- Índices para pedidos
CREATE INDEX idx_pedidos_cliente ON pedidos(cliente_id);
CREATE INDEX idx_pedidos_fecha ON pedidos(fecha_pedido);
CREATE INDEX idx_pedidos_estado ON pedidos(estado);

-- Índices para compatibilidades
CREATE INDEX idx_compatibilidades_producto ON compatibilidades(producto_sku);
CREATE INDEX idx_compatibilidades_vehiculo ON compatibilidades(vehiculo_id);

-- Índices para ventas
CREATE INDEX idx_ventas_fecha ON ventas(fecha_venta);
CREATE INDEX idx_ventas_metodo_pago ON ventas(metodo_pago);

-- =============================================
-- VISTAS ÚTILES
-- =============================================

CREATE OR REPLACE VIEW vista_inventario_critico AS
SELECT 
    p.sku,
    p.nombre,
    p.numero_parte,
    c.nombre as categoria,
    i.stock_actual,
    i.stock_minimo,
    i.ubicacion,
    (i.stock_minimo - i.stock_actual) as faltante,
    CASE 
        WHEN i.stock_actual = 0 THEN 'SIN STOCK'
        WHEN i.stock_actual <= i.stock_minimo THEN 'STOCK CRÍTICO'
        ELSE 'STOCK NORMAL'
    END as estado_stock
FROM productos p
JOIN inventario i ON p.sku = i.producto_sku
JOIN categorias c ON p.categoria_id = c.id
WHERE i.stock_actual <= i.stock_minimo AND p.activo = true
ORDER BY faltante DESC;

CREATE OR REPLACE VIEW vista_ventas_mensuales AS
SELECT 
    TO_CHAR(v.fecha_venta, 'YYYY-MM') as mes,
    COUNT(*) as total_ventas,
    SUM(v.total) as ingresos_totales,
    AVG(v.total) as ticket_promedio,
    COUNT(DISTINCT p.cliente_id) as clientes_unicos
FROM ventas v
JOIN pedidos p ON v.pedido_id = p.id
GROUP BY TO_CHAR(v.fecha_venta, 'YYYY-MM')
ORDER BY mes DESC;

CREATE OR REPLACE VIEW vista_productos_populares AS
SELECT 
    p.sku,
    p.nombre,
    c.nombre as categoria,
    COUNT(dp.id) as total_ventas,
    SUM(dp.cantidad) as unidades_vendidas,
    SUM(dp.subtotal_linea) as ingresos_totales
FROM productos p
JOIN detalle_pedidos dp ON p.sku = dp.producto_sku
JOIN categorias c ON p.categoria_id = c.id
JOIN pedidos ped ON dp.pedido_id = ped.id
JOIN ventas v ON ped.id = v.pedido_id
GROUP BY p.sku, p.nombre, c.nombre
ORDER BY unidades_vendidas DESC;

-- =============================================
-- FUNCIONES Y TRIGGERS
-- =============================================

-- Función para actualizar timestamps
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Triggers para updated_at
CREATE TRIGGER update_productos_updated_at 
    BEFORE UPDATE ON productos 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_inventario_updated_at 
    BEFORE UPDATE ON inventario 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_clientes_updated_at 
    BEFORE UPDATE ON clientes 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_pedidos_updated_at 
    BEFORE UPDATE ON pedidos 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_proveedores_updated_at 
    BEFORE UPDATE ON proveedores 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Función para calcular totales de pedido
CREATE OR REPLACE FUNCTION calcular_totales_pedido()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE pedidos 
    SET subtotal = COALESCE((
        SELECT SUM(subtotal_linea) 
        FROM detalle_pedidos 
        WHERE pedido_id = NEW.pedido_id
    ), 0),
    iva = subtotal * 0.16,
    total = subtotal + iva
    WHERE id = NEW.pedido_id;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_calcular_totales
    AFTER INSERT OR UPDATE OR DELETE ON detalle_pedidos
    FOR EACH ROW EXECUTE FUNCTION calcular_totales_pedido();

-- Función para alertas de stock
CREATE OR REPLACE FUNCTION check_stock_alerts()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.stock_actual <= NEW.stock_minimo THEN
        RAISE NOTICE 'ALERTA DE STOCK: Producto % (%): Stock actual % (Mínimo: %)', 
            NEW.producto_sku, 
            (SELECT nombre FROM productos WHERE sku = NEW.producto_sku),
            NEW.stock_actual, 
            NEW.stock_minimo;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_stock_alerts 
    AFTER INSERT OR UPDATE ON inventario
    FOR EACH ROW EXECUTE FUNCTION check_stock_alerts();

-- =============================================
-- DATOS DE EJEMPLO
-- =============================================

-- Insertar categorías básicas
INSERT INTO categorias (nombre, descripcion) VALUES 
('Filtros', 'Filtros de aceite, aire, combustible y habitáculo'),
('Frenos', 'Sistema de frenos: pastillas, discos, tambores'),
('Motor', 'Componentes del motor y sistemas relacionados'),
('Transmisión', 'Sistemas de transmisión y embrague'),
('Eléctrico', 'Sistema eléctrico y electrónico'),
('Suspensión', 'Sistema de suspensión y dirección'),
('Escapes', 'Sistema de escape y catalizadores'),
('Lubricantes', 'Aceites, grasas y fluidos')
ON CONFLICT (nombre) DO NOTHING;

-- Insertar marcas de vehículos
INSERT INTO marcas_vehiculos (nombre, pais_origen) VALUES 
('Toyota', 'Japón'),
('Nissan', 'Japón'),
('Volkswagen', 'Alemania'),
('Ford', 'Estados Unidos'),
('Chevrolet', 'Estados Unidos'),
('Honda', 'Japón'),
('BMW', 'Alemania'),
('Mercedes-Benz', 'Alemania')
ON CONFLICT (nombre) DO NOTHING;

-- Insertar proveedores
INSERT INTO proveedores (nombre, contacto, telefono, email, terminos_pago) VALUES 
('AutoParts Supply Co.', 'Juan Martínez', '+52-55-1234-5678', 'ventas@autopartssupply.com', '30 días'),
('MotorTech International', 'María González', '+52-55-2345-6789', 'mgonzalez@motortech.com', '15 días'),
('Brake Systems MX', 'Carlos Rodríguez', '+52-55-3456-7890', 'crodriguez@brakesystems.mx', '45 días')
ON CONFLICT DO NOTHING;

-- =============================================
-- MENSAJE FINAL
-- =============================================

DO $$ 
BEGIN
    RAISE NOTICE '===============================================';
    RAISE NOTICE 'ESQUEMA AUTOPARTS CREADO EXITOSAMENTE';
    RAISE NOTICE '===============================================';
    RAISE NOTICE 'Tablas creadas: 10';
    RAISE NOTICE 'Índices creados: 15';
    RAISE NOTICE 'Vistas creadas: 3';
    RAISE NOTICE 'Funciones/Triggers: 8';
    RAISE NOTICE '===============================================';
END $$;