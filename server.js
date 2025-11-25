const WebSocket = require('ws');
const express = require('express');
const http = require('http');
const sqlite3 = require('sqlite3').verbose();
const path = require('path');
const crypto = require('crypto');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// Configuración
const PORT = process.env.PORT || 3000;
const CLIENTS = new Map(); // Mejor que Set para más control
let connectionIdCounter = 1;

// Middleware
app.use(express.json());
app.use(express.static('public'));

// Headers CORS
app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
    res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization');
    next();
});

// Base de datos SQLite
const db = new sqlite3.Database('./guasitos.db', (err) => {
    if (err) {
        console.error('❌ Error con SQLite:', err.message);
    } else {
        console.log('✅ Conectado a SQLite database.');
        initializeDatabase();
    }
});

// Esquema de la base de datos (igual que tu worker)
const DB_TABLES = [
    {
        name: 'users',
        schema: `CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            name TEXT NOT NULL,
            phone TEXT,
            role TEXT DEFAULT 'user',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )`
    },
    {
        name: 'urgentes',
        schema: `CREATE TABLE IF NOT EXISTS urgentes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            description TEXT NOT NULL,
            image TEXT,
            author_id INTEGER NOT NULL,
            views INTEGER DEFAULT 0,
            likes INTEGER DEFAULT 0,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )`
    },
    // ... Agrega todas las demás tablas de tu worker
];

function initializeDatabase() {
    console.log('🔄 Inicializando base de datos...');
    
    DB_TABLES.forEach(table => {
        db.run(table.schema, (err) => {
            if (err) {
                console.error(`❌ Error creando ${table.name}:`, err.message);
            } else {
                console.log(`✅ Tabla ${table.name} lista`);
            }
        });
    });
    
    // Crear usuario admin por defecto
    db.get("SELECT 1 FROM users WHERE username = 'guantanamo'", (err, row) => {
        if (!row) {
            const passwordHash = hashPassword('cuba123');
            db.run(
                `INSERT INTO users (username, password_hash, name, phone, role) VALUES (?, ?, ?, ?, ?)`,
                ['guantanamo', passwordHash, 'Administrador', '+53 12345678', 'admin']
            );
            console.log('✅ Usuario admin creado');
        }
    });
}

// Utilidades
function hashPassword(password) {
    return crypto.createHash('sha256')
        .update(password + 'guasitos-salt-2024')
        .digest('hex');
}

function generateSessionToken() {
    return crypto.randomUUID();
}

// WebSocket mejorado
wss.on('connection', (ws, req) => {
    const connectionId = connectionIdCounter++;
    const clientInfo = {
        id: connectionId,
        socket: ws,
        connectedAt: new Date(),
        lastActivity: new Date(),
        ip: req.socket.remoteAddress,
        userAgent: req.headers['user-agent'] || 'Unknown',
        authenticated: false,
        subscribedToUpdates: false,
        userId: null,
        userName: null
    };

    CLIENTS.set(connectionId, clientInfo);
    
    console.log(`✅ Cliente #${connectionId} conectado desde ${clientInfo.ip}. Total: ${CLIENTS.size}`);

    // Mensaje de bienvenida
    ws.send(JSON.stringify({
        type: 'welcome',
        connectionId: connectionId,
        message: 'Conectado al servidor Guasitos',
        timestamp: new Date().toISOString(),
        activeConnections: CLIENTS.size
    }));

    // Manejar mensajes
    ws.on('message', async (data) => {
        try {
            clientInfo.lastActivity = new Date();
            const message = JSON.parse(data);
            await handleWebSocketMessage(connectionId, message);
        } catch (error) {
            console.error('❌ Error procesando mensaje:', error);
            ws.send(JSON.stringify({
                type: 'error',
                message: 'Error procesando mensaje'
            }));
        }
    });

    // Manejar desconexión
    ws.on('close', () => {
        console.log(`🔌 Cliente #${connectionId} desconectado`);
        CLIENTS.delete(connectionId);
        console.log(`📊 Conexiones activas: ${CLIENTS.size}`);
    });

    // Manejar errores
    ws.on('error', (error) => {
        console.error(`💥 Error en cliente #${connectionId}:`, error);
        CLIENTS.delete(connectionId);
    });
});

// Manejar mensajes WebSocket
async function handleWebSocketMessage(connectionId, message) {
    const client = CLIENTS.get(connectionId);
    if (!client) return;

    console.log(`📨 Mensaje de #${connectionId}: ${message.type}`);

    switch (message.type) {
        case 'ping':
            client.socket.send(JSON.stringify({
                type: 'pong',
                timestamp: new Date().toISOString()
            }));
            break;

        case 'subscribe_updates':
            client.subscribedToUpdates = true;
            client.socket.send(JSON.stringify({
                type: 'subscribed',
                message: 'Suscrito a actualizaciones en tiempo real'
            }));
            break;

        case 'get_data':
            await handleGetDataRequest(client);
            break;

        case 'authenticate':
            await handleAuthentication(client, message);
            break;

        case 'broadcast_message':
            if (client.authenticated) {
                broadcast(JSON.stringify({
                    type: 'chat_message',
                    from: client.userName,
                    message: message.content,
                    timestamp: new Date().toISOString()
                }));
            }
            break;

        default:
            console.log('❓ Mensaje no reconocido:', message.type);
    }
}

// Obtener datos de la base de datos
async function handleGetDataRequest(client) {
    return new Promise((resolve) => {
        // Consultar datos de diferentes tablas
        const queries = {
            urgentes: "SELECT * FROM urgentes ORDER BY created_at DESC LIMIT 50",
            zonas: "SELECT * FROM zonas ORDER BY created_at DESC LIMIT 50",
            users: "SELECT id, username, name, role FROM users ORDER BY created_at DESC LIMIT 50"
        };

        const results = {};
        let completed = 0;
        const total = Object.keys(queries).length;

        Object.entries(queries).forEach(([key, query]) => {
            db.all(query, [], (err, rows) => {
                if (err) {
                    console.error(`Error en query ${key}:`, err);
                    results[key] = [];
                } else {
                    results[key] = rows;
                }
                
                completed++;
                if (completed === total) {
                    // Enviar datos al cliente
                    client.socket.send(JSON.stringify({
                        type: 'data_response',
                        data: results,
                        timestamp: new Date().toISOString()
                    }));
                    resolve();
                }
            });
        });
    });
}

// Autenticación
async function handleAuthentication(client, message) {
    const { username, password, session_token } = message;

    if (session_token) {
        // Verificar sesión existente
        db.get(
            `SELECT u.id, u.username, u.name, u.role 
             FROM user_sessions s 
             JOIN users u ON s.user_id = u.id 
             WHERE s.session_token = ? AND s.expires_at > datetime('now')`,
            [session_token],
            (err, session) => {
                if (session) {
                    client.authenticated = true;
                    client.userId = session.id;
                    client.userName = session.name;
                    
                    client.socket.send(JSON.stringify({
                        type: 'authenticated',
                        user: {
                            id: session.id,
                            name: session.name,
                            role: session.role
                        }
                    }));
                }
            }
        );
    } else if (username && password) {
        // Login con usuario/contraseña
        const passwordHash = hashPassword(password);
        
        db.get(
            "SELECT id, username, name, role FROM users WHERE username = ? AND password_hash = ?",
            [username, passwordHash],
            (err, user) => {
                if (user) {
                    const sessionToken = generateSessionToken();
                    const expiresAt = new Date(Date.now() + 7 * 24 * 60 * 60 * 1000);
                    
                    db.run(
                        "INSERT INTO user_sessions (user_id, session_token, expires_at) VALUES (?, ?, ?)",
                        [user.id, sessionToken, expiresAt.toISOString()]
                    );
                    
                    client.authenticated = true;
                    client.userId = user.id;
                    client.userName = user.name;
                    
                    client.socket.send(JSON.stringify({
                        type: 'authenticated',
                        user: {
                            id: user.id,
                            name: user.name,
                            role: user.role
                        },
                        session_token: sessionToken
                    }));
                } else {
                    client.socket.send(JSON.stringify({
                        type: 'auth_error',
                        message: 'Credenciales inválidas'
                    }));
                }
            }
        );
    }
}

// Broadcast mejorado
function broadcast(message, options = {}) {
    const messageStr = typeof message === 'string' ? message : JSON.stringify(message);
    let sentCount = 0;
    
    CLIENTS.forEach((client, id) => {
        try {
            if (client.socket.readyState === client.socket.OPEN) {
                // Filtros opcionales
                if (options.onlyAuthenticated && !client.authenticated) return;
                if (options.onlySubscribed && !client.subscribedToUpdates) return;
                
                client.socket.send(messageStr);
                sentCount++;
            }
        } catch (error) {
            console.error(`Error enviando a cliente #${id}:`, error);
        }
    });
    
    console.log(`📤 Broadcast enviado a ${sentCount} clientes`);
    return sentCount;
}

// Endpoints HTTP
app.get('/api/test', (req, res) => {
    res.json({
        message: 'Servidor Guasitos funcionando',
        timestamp: new Date().toISOString(),
        activeConnections: CLIENTS.size,
        version: '2.0.0'
    });
});

app.get('/api/stats', (req, res) => {
    const stats = {
        totalConnections: CLIENTS.size,
        authenticated: Array.from(CLIENTS.values()).filter(c => c.authenticated).length,
        connections: Array.from(CLIENTS.values()).map(c => ({
            id: c.id,
            authenticated: c.authenticated,
            userName: c.userName,
            connectedSince: c.connectedAt,
            ip: c.ip
        }))
    };
    res.json(stats);
});

// Endpoint para servir el frontend
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Limpieza periódica de conexiones inactivas
setInterval(() => {
    const now = new Date();
    let cleaned = 0;
    
    CLIENTS.forEach((client, id) => {
        const inactiveTime = now - client.lastActivity;
        if (inactiveTime > 120000) { // 2 minutos
            console.log(`🧹 Limpiando cliente inactivo #${id}`);
            client.socket.close();
            CLIENTS.delete(id);
            cleaned++;
        }
    });
    
    if (cleaned > 0) {
        console.log(`🧹 Limpiados ${cleaned} clientes inactivos`);
    }
}, 60000); // Cada minuto

// Notificaciones periódicas (opcional)
setInterval(() => {
    broadcast({
        type: 'notification',
        message: 'Servidor activo',
        timestamp: new Date().toISOString(),
        activeUsers: CLIENTS.size
    }, { onlySubscribed: true });
}, 30000);

// Iniciar servidor
server.listen(PORT, () => {
    console.log(`🚀 Servidor Guasitos ejecutándose en puerto ${PORT}`);
    console.log(`📡 WebSocket: ws://localhost:${PORT}`);
    console.log(`🌐 HTTP: http://localhost:${PORT}`);
    console.log(`📊 Estadísticas: http://localhost:${PORT}/api/stats`);
});
