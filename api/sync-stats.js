import { Pool } from 'pg';

// Create PostgreSQL connection pool
const pool = new Pool({
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  database: process.env.DB_DATABASE,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
});

export default async function handler(req, res) {
  // Only allow POST requests
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  // Basic API key authentication (optional but recommended)
  const apiKey = req.headers['x-api-key'];
  if (apiKey !== process.env.API_KEY) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  try {
    const { players } = req.body;

    if (!players || !Array.isArray(players)) {
      return res.status(400).json({ error: 'Invalid data format. Expected { players: [...] }' });
    }

    const client = await pool.connect();

    try {
      // Start transaction
      await client.query('BEGIN');

      // Prepare the upsert SQL
      const sql = `
        INSERT INTO player_stats (
          player_name, team_name, status, kills, deaths, assists, kda, games_played,
          avg_kills, avg_deaths, avg_assists, avg_kda, cs, avg_cs, last_updated
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, NOW())
        ON CONFLICT (player_name, team_name) DO UPDATE SET
          status = EXCLUDED.status,
          kills = EXCLUDED.kills,
          deaths = EXCLUDED.deaths,
          assists = EXCLUDED.assists,
          kda = EXCLUDED.kda,
          games_played = EXCLUDED.games_played,
          avg_kills = EXCLUDED.avg_kills,
          avg_deaths = EXCLUDED.avg_deaths,
          avg_assists = EXCLUDED.avg_assists,
          avg_kda = EXCLUDED.avg_kda,
          cs = EXCLUDED.cs,
          avg_cs = EXCLUDED.avg_cs,
          last_updated = NOW()
      `;

      let processed = 0;
      
      // Process each player
      for (const player of players) {
        const values = [
          player.name,
          player.teamName,
          player.status,
          parseInt(player.kills) || 0,
          parseInt(player.deaths) || 0,
          parseInt(player.assists) || 0,
          parseFloat(player.kda) || 0,
          parseInt(player.games) || 0,
          parseFloat(player.avgKills) || 0,
          parseFloat(player.avgDeaths) || 0,
          parseFloat(player.avgAssists) || 0,
          parseFloat(player.avgKda) || 0,
          parseInt(player.cs) || 0,
          parseFloat(player.avgCs) || 0
        ];

        await client.query(sql, values);
        processed++;
      }

      // Commit transaction
      await client.query('COMMIT');

      res.status(200).json({ 
        success: true, 
        message: `Successfully processed ${processed} players`,
        processed: processed
      });

    } catch (error) {
      // Rollback on error
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }

  } catch (error) {
    console.error('Database error:', error);
    res.status(500).json({ 
      error: 'Database operation failed', 
      details: error.message 
    });
  }
}
