const express = require('express');
const cors = require('cors');
const http = require('http');
const crypto = require('crypto');
const { spawn } = require('child_process');
const { Pool } = require('pg');
const { createClient } = require('@supabase/supabase-js');
const ws = require('ws');

// ============================================================
// CONFIGURAÇÃO CENTRAL DO SERVIDOR
// Altere SOMENTE esta constante se futuramente precisar mudar
// a porta de todo o sistema.
// ============================================================
const PORT = 5000;

const app = express();
app.use(cors());
app.use(express.json());

// ============================================================
// SUPABASE
// ============================================================
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_KEY;
if (!supabaseUrl || !supabaseKey) {
    throw new Error('Configure as variáveis de ambiente SUPABASE_URL e SUPABASE_KEY.');
}
const supabase = createClient(supabaseUrl, supabaseKey, {
    realtime: {
        transport: ws
    }
});

// ============================================================
// POSTGRESQL / ERP VR
// ============================================================
const pool = new Pool({
    host: process.env.ERP_DB_HOST,
    port: Number(process.env.ERP_DB_PORT || 8745),
    database: process.env.ERP_DB_NAME || 'vr',
    user: process.env.ERP_DB_USER,
    password: process.env.ERP_DB_PASSWORD,
    max: 5,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 10000
});

// ============================================================
// UTILITÁRIOS
// ============================================================
function validarData(data) {
    if (typeof data !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(data)) {
        return false;
    }

    const [ano, mes, dia] = data.split('-').map(Number);
    const d = new Date(ano, mes - 1, dia);

    return d.getFullYear() === ano &&
           d.getMonth() === mes - 1 &&
           d.getDate() === dia;
}

function validarIntervalo(dataInicial, dataFinal) {
    return validarData(dataInicial) &&
           validarData(dataFinal) &&
           dataInicial <= dataFinal;
}

// ============================================================
// MÓDULO: CONCILIADOR / FORNECEDORES
// ============================================================
const queryConciliacao = `
WITH tab_parametro_data AS (
    SELECT CAST ($1 AS DATE) AS data_final
),
tab_conta_conciliar AS (
    SELECT id AS conta FROM contacontabilfiscal 
    WHERE conta1= 2 AND conta2= 1 AND conta3= 1 AND conta4 = 1 AND nivel = 5 AND id_situacaocadastro = 1
), 
conciliacao_banco AS (
    (SELECT 
            f.id_contacontabilfiscalpassivo AS id_contacontabilfiscal, 
            p.id_loja, 
            COALESCE(SUM(pp.valor) - COALESCE(SUM(pa.valor), 0), 0) AS saldofinanceiro, 
            0 AS valorsaldocontabil
    FROM fornecedor f 
    JOIN pagarfornecedor p ON p.id_fornecedor = f.id
    LEFT JOIN pagarfornecedorparcela pp ON p.id = pp.id_pagarfornecedor
    LEFT JOIN pagarfornecedorparcelaabatimento pa ON pa.id_pagarfornecedorparcela = pp.id AND pa.id_tipoabatimento = 3
    WHERE f.id_contacontabilfiscalpassivo IN (SELECT conta FROM tab_conta_conciliar)
      AND (p.dataentrada <= (SELECT data_final FROM tab_parametro_data)
           AND (pp.datapagamento > (SELECT data_final FROM tab_parametro_data) OR pp.datapagamento IS NULL))
    GROUP BY f.id_contacontabilfiscalpassivo, p.id_loja)

    UNION ALL

    (SELECT 
            cl.id_contacontabilfiscal, 
            c.id_loja,  
            0 AS saldofinanceiro,
            SUM(-cl.valordebito) + SUM(cl.valorcredito) AS valorsaldocontabil
    FROM contabilidadelancamento cl  
    LEFT JOIN contabilidade c ON c.id = cl.id_contabilidade
    WHERE cl.id_contacontabilfiscal IN (SELECT conta FROM tab_conta_conciliar)
      AND c.data <= (SELECT data_final FROM tab_parametro_data)
    GROUP BY cl.id_contacontabilfiscal, c.id_loja)
),
tab_conciliacao_banco AS (
    SELECT 
        cl.id_contacontabilfiscal, 
        cl.id_loja, 
        SUM(cl.saldofinanceiro) AS saldofinanceiro, 
        SUM(cl.valorsaldocontabil) AS valorsaldocontabil, 
        SUM(cl.saldofinanceiro - cl.valorsaldocontabil) AS comparativo
    FROM conciliacao_banco cl
    GROUP BY cl.id_contacontabilfiscal, cl.id_loja
)
SELECT 
    ccf.conta1 ||'.'||ccf.conta2 ||'.'||LPAD(CAST(ccf.conta3 AS VARCHAR), 2, '0')
             ||'.'||LPAD(CAST(ccf.conta4 AS VARCHAR), 2, '0') ||'.'||LPAD(CAST(ccf.conta5 AS VARCHAR), 6, '0') AS conta_mascara,
    valores.id_contacontabilfiscal AS conta_contabil,   
    valores.id_loja AS idloja,   
    CASE 
        WHEN fornc.rural_count > 0 THEN 'Sim' 
        ELSE 'Não' 
    END AS produtor_rural,
    valores.valorsaldocontabil AS saldo_contabil,
    COALESCE(valores.saldofinanceiro, 0.00) AS saldo_financeiro,      
    COALESCE(valores.comparativo, 0.00) AS comparativo,
    l.descricao AS loja,
    ccf.descricao AS nome_conta
FROM tab_conciliacao_banco valores
LEFT JOIN contacontabilfiscal ccf ON ccf.id = valores.id_contacontabilfiscal
LEFT JOIN loja l ON l.id = valores.id_loja
LEFT JOIN (
    SELECT id_contacontabilfiscalpassivo, 
           SUM(CASE WHEN descontofunrural THEN 1 ELSE 0 END) as rural_count
    FROM fornecedor 
    GROUP BY id_contacontabilfiscalpassivo
) fornc ON fornc.id_contacontabilfiscalpassivo = valores.id_contacontabilfiscal
WHERE valores.comparativo <> 0
ORDER BY loja, conta_contabil;
`;

async function executarConciliacao(dataFinal) {
    let client;

    try {
        console.log(`\n[FORNECEDORES] Competência: ${dataFinal}`);
        client = await pool.connect();

        const resultado = await client.query(queryConciliacao, [dataFinal]);
        const consultaId = crypto.randomUUID();
        const dataConsulta = new Date().toISOString();

        const registros = resultado.rows.map(row => ({
            consulta_id: consultaId,
            data_final: dataFinal,
            data_consulta: dataConsulta,
            conta_mascara: row.conta_mascara ?? null,
            conta_contabil: row.conta_contabil ?? null,
            idloja: row.idloja ?? null,
            produtor_rural: row.produtor_rural ?? null,
            saldo_contabil: row.saldo_contabil ?? 0,
            saldo_financeiro: row.saldo_financeiro ?? 0,
            comparativo: row.comparativo ?? 0,
            loja: row.loja ?? null,
            nome_conta: row.nome_conta ?? null
        }));

        // IMPORTANTE: não apaga mais o histórico.
        // Cada consulta recebe um consulta_id próprio e permanece no Supabase.
        if (registros.length > 0) {
            const { error: insertError } = await supabase
                .from('relatorio_fornecedores')
                .insert(registros);

            if (insertError) {
                throw new Error(`Erro ao inserir no Supabase: ${insertError.message}`);
            }
        }

        console.log(`[FORNECEDORES] Consulta ${consultaId} salva com ${registros.length} registro(s).`);

        return {
            sucesso: true,
            total: registros.length,
            dataFinal,
            consultaId,
            dados: registros,
            erro: null
        };
    } finally {
        if (client) client.release();
    }
}

app.post('/executar-conciliacao', async (req, res) => {
    const dataFinal = String(req.body?.dataFinal || '').trim();

    console.log('\nNova solicitação: /executar-conciliacao');

    if (!validarData(dataFinal)) {
        return res.status(400).json({
            sucesso: false,
            total: 0,
            dataFinal,
            erro: 'Data final inválida. Informe a data no formato YYYY-MM-DD.'
        });
    }

    try {
        return res.json(await executarConciliacao(dataFinal));
    } catch (erro) {
        console.error('[FORNECEDORES] ERRO:', erro);
        return res.status(500).json({
            sucesso: false,
            total: 0,
            dataFinal,
            erro: erro.message || 'Erro desconhecido.'
        });
    }
});

// ============================================================
// MÓDULO: CONCILIAÇÃO FISCAL - REVENDA DE MERCADORIAS
// Consulta fiscal + contábil para o primeiro cruzamento fiscal.
// A loja e o período são escolhidos na tela e enviados para esta rota.
// O cruzamento exige uma única loja por consulta.
// Mapa Resumo, CFOPs e contábil retornam em blocos separados.
// ============================================================
const queryConciliacaoFiscalRevenda = `
WITH parametros AS (
    SELECT
        CAST($1 AS DATE) AS data_inicio,
        CAST($2 AS DATE) AS data_final,
        CAST($3 AS INTEGER) AS id_loja
),

contas_filtro AS (
    SELECT id AS conta
    FROM contacontabilfiscal
    WHERE conta1 = 3
      AND conta2 = 1
      AND conta3 = '1'
      AND conta4 = '1'
      AND nivel = 5
      AND id_situacaocadastro = 1
),

total_fiscal_mapa AS (
    SELECT
        mr.id_loja,
        l.descricao AS nome_loja,
        SUM(mr.gtfinal - mr.gtinicial - mr.cancelamento - mr.desconto) AS total_saida_mapa
    FROM maparesumo mr
    INNER JOIN loja l ON l.id = mr.id_loja
    CROSS JOIN parametros p
    WHERE mr.data >= p.data_inicio
      AND mr.data < p.data_final + INTERVAL '1 day'
      AND mr.id_loja = p.id_loja
    GROUP BY mr.id_loja, l.descricao
),

total_fiscal_item AS (
    SELECT
        nf.id_loja,
        l.descricao AS nome_loja,
        nfi.cfop,
        SUM(nfi.valortotal) AS total_saida_item
    FROM notasaidaitem nfi
    INNER JOIN notasaida nf ON nf.id = nfi.id_notasaida
    INNER JOIN loja l ON l.id = nf.id_loja
    CROSS JOIN parametros p
    WHERE nf.datasaida >= p.data_inicio
      AND nf.datasaida < p.data_final + INTERVAL '1 day'
      AND nf.id_loja = p.id_loja
      AND nf.id_situacaonfe = 1
    GROUP BY nf.id_loja, l.descricao, nfi.cfop
),

total_contabil AS (
    SELECT
        c.id_loja,
        l.descricao AS nome_loja,
        SUM(cl.valordebito) AS total_debito,
        SUM(cl.valorcredito) AS total_credito
    FROM contabilidadelancamento cl
    INNER JOIN contabilidade c ON c.id = cl.id_contabilidade
    INNER JOIN loja l ON l.id = c.id_loja
    CROSS JOIN parametros p
    WHERE cl.id_contacontabilfiscal IN (SELECT conta FROM contas_filtro)
      AND c.data >= p.data_inicio
      AND c.data < p.data_final + INTERVAL '1 day'
      AND c.id_loja = p.id_loja
    GROUP BY c.id_loja, l.descricao
)

SELECT * FROM (
    SELECT
        'FISCAL_MAPA_RESUMO'::TEXT AS tipo_resumo,
        tfm.id_loja AS id_loja,
        tfm.nome_loja,
        'TOTAL_MAPA_RESUMO'::TEXT AS codigo_referencia,
        tfm.total_saida_mapa AS valor_total_fiscal,
        NULL::NUMERIC AS total_debito_contabil,
        NULL::NUMERIC AS total_credito_contabil,
        NULL::NUMERIC AS diferenca_fiscal_contabil,
        NULL::TEXT AS situacao_nfe
    FROM total_fiscal_mapa tfm

    UNION ALL

    SELECT
        'FISCAL_CFOP_DETALHE'::TEXT AS tipo_resumo,
        tfi.id_loja AS id_loja,
        tfi.nome_loja,
        tfi.cfop::TEXT AS codigo_referencia,
        tfi.total_saida_item AS valor_total_fiscal,
        NULL::NUMERIC AS total_debito_contabil,
        NULL::NUMERIC AS total_credito_contabil,
        NULL::NUMERIC AS diferenca_fiscal_contabil,
        '1'::TEXT AS situacao_nfe
    FROM total_fiscal_item tfi

    UNION ALL

    SELECT
        'CONTABIL'::TEXT AS tipo_resumo,
        tc.id_loja AS id_loja,
        tc.nome_loja,
        'GERAL'::TEXT AS codigo_referencia,
        NULL::NUMERIC AS valor_total_fiscal,
        tc.total_debito AS total_debito_contabil,
        tc.total_credito AS total_credito_contabil,
        NULL::NUMERIC AS diferenca_fiscal_contabil,
        NULL::TEXT AS situacao_nfe
    FROM total_contabil tc
) AS resultados
ORDER BY nome_loja, tipo_resumo DESC, codigo_referencia;
`;

async function executarConciliacaoFiscalRevenda(dataInicial, dataFinal, idLoja) {
    let client;

    try {
        console.log(`\n[CONCILIAÇÃO FISCAL - REVENDA] Período: ${dataInicial} até ${dataFinal} | Loja ID: ${idLoja}`);
        client = await pool.connect();

        const resultado = await client.query(
            queryConciliacaoFiscalRevenda,
            [dataInicial, dataFinal, idLoja]
        );

        console.log(`[CONCILIAÇÃO FISCAL - REVENDA] ${resultado.rowCount} registro(s) retornado(s).`);

        return {
            sucesso: true,
            tipo: 'REVENDA_MERCADORIAS',
            dataInicial,
            dataFinal,
            total: resultado.rowCount,
            dados: resultado.rows,
            erro: null
        };
    } finally {
        if (client) client.release();
    }
}

app.post('/executar-conciliacao-fiscal', async (req, res) => {
    const dataInicial = String(req.body?.dataInicial || '').trim();
    const dataFinal = String(req.body?.dataFinal || '').trim();
    const lojaRaw = req.body?.idLoja ?? req.body?.lojaId ?? '';
    const idLoja = lojaRaw === '' || lojaRaw === null || lojaRaw === undefined || String(lojaRaw).trim().toUpperCase() === 'TODAS'
        ? null
        : Number(lojaRaw);

    console.log(`\nNova solicitação: /executar-conciliacao-fiscal | Loja: ${idLoja ?? 'NÃO INFORMADA'}`);

    if (idLoja === null) {
        return res.status(400).json({
            sucesso: false,
            tipo: 'REVENDA_MERCADORIAS',
            total: 0,
            dados: [],
            dataInicial,
            dataFinal,
            erro: 'Selecione uma loja na tela. O cruzamento de vendas consulta uma loja por vez.'
        });
    }

    if (!Number.isInteger(idLoja) || idLoja <= 0) {
        return res.status(400).json({
            sucesso: false,
            tipo: 'REVENDA_MERCADORIAS',
            total: 0,
            dados: [],
            dataInicial,
            dataFinal,
            idLoja,
            erro: 'Loja inválida. Informe o ID numérico da loja selecionada na tela.'
        });
    }

    if (!validarIntervalo(dataInicial, dataFinal)) {
        return res.status(400).json({
            sucesso: false,
            tipo: 'REVENDA_MERCADORIAS',
            total: 0,
            dados: [],
            dataInicial,
            dataFinal,
            erro: 'Intervalo de datas inválido. Informe datas YYYY-MM-DD e a data inicial não pode ser maior que a final.'
        });
    }

    try {
        return res.json(await executarConciliacaoFiscalRevenda(dataInicial, dataFinal, idLoja));
    } catch (erro) {
        console.error('[CONCILIAÇÃO FISCAL - REVENDA] ERRO:', erro);
        return res.status(500).json({
            sucesso: false,
            tipo: 'REVENDA_MERCADORIAS',
            total: 0,
            dados: [],
            dataInicial,
            dataFinal,
            idLoja,
            erro: erro.message || 'Erro desconhecido na consulta fiscal de revenda.'
        });
    }
});

// ============================================================
// MÓDULO: CONCILIAÇÃO FISCAL - PARTE CONTÁBIL
// Esta consulta traz SOMENTE os valores contábeis.
// O CFOP, valor fiscal e comparativo serão alimentados pela
// consulta fiscal quando ela for integrada.
// ============================================================
const queryConciliacaoFiscalContabil = `
WITH parametros AS (
    SELECT
        CAST($1 AS DATE) AS data_inicio,
        CAST($2 AS DATE) AS data_final
),

contas_filtro AS (
    SELECT id AS conta
    FROM contacontabilfiscal
    WHERE conta1 = 3
      AND conta2 = 1
      AND conta3 = '1'
      AND conta4 = '1'
      AND nivel = 5
      AND id_situacaocadastro = 1
)

SELECT
    l.descricao AS loja,
    COALESCE(SUM(cl.valordebito), 0) AS total_debito,
    COALESCE(SUM(cl.valorcredito), 0) AS total_credito,
    COALESCE(SUM(cl.valordebito), 0)
        - COALESCE(SUM(cl.valorcredito), 0) AS saldo
FROM contabilidadelancamento cl
INNER JOIN contabilidade c
    ON c.id = cl.id_contabilidade
INNER JOIN loja l
    ON l.id = c.id_loja
CROSS JOIN parametros p
WHERE cl.id_contacontabilfiscal IN (
    SELECT conta
    FROM contas_filtro
)
AND c.data BETWEEN p.data_inicio AND p.data_final
GROUP BY l.descricao
ORDER BY l.descricao;
`;

async function executarConciliacaoFiscalContabil(dataInicial, dataFinal) {
    let client;

    try {
        console.log(`\\n[CONCILIAÇÃO FISCAL] Parte contábil: ${dataInicial} até ${dataFinal}`);
        client = await pool.connect();

        const resultado = await client.query(
            queryConciliacaoFiscalContabil,
            [dataInicial, dataFinal]
        );

        console.log(`[CONCILIAÇÃO FISCAL] ${resultado.rowCount} loja(s) com dados contábeis.`);

        return {
            sucesso: true,
            dataInicial,
            dataFinal,
            total: resultado.rowCount,
            dados: resultado.rows,
            erro: null
        };
    } finally {
        if (client) client.release();
    }
}

// Consulta a parte contábil da Conciliação Fiscal.
// Não grava no Supabase ainda, porque esses dados precisam ser
// combinados com a consulta FISCAL antes de formar o comparativo.
app.post('/executar-conciliacao-fiscal-contabil', async (req, res) => {
    const dataInicial = String(req.body?.dataInicial || '').trim();
    const dataFinal = String(req.body?.dataFinal || '').trim();

    console.log('\\nNova solicitação: /executar-conciliacao-fiscal-contabil');

    if (!validarIntervalo(dataInicial, dataFinal)) {
        return res.status(400).json({
            sucesso: false,
            total: 0,
            dados: [],
            dataInicial,
            dataFinal,
            erro: 'Intervalo de datas inválido. Informe datas YYYY-MM-DD e a inicial não pode ser maior que a final.'
        });
    }

    try {
        return res.json(await executarConciliacaoFiscalContabil(dataInicial, dataFinal));
    } catch (erro) {
        console.error('[CONCILIAÇÃO FISCAL] ERRO CONTÁBIL:', erro);
        return res.status(500).json({
            sucesso: false,
            total: 0,
            dados: [],
            dataInicial,
            dataFinal,
            erro: erro.message || 'Erro desconhecido na consulta contábil.'
        });
    }
});

// ============================================================
// MÓDULO: MAPA RESUMO
// ============================================================
const queryMapaResumo = `
WITH resumo_mapas AS (
    SELECT 
        id_loja,
        data,
        SUM((gtfinal - gtinicial) - cancelamento - desconto) AS valor_venda
    FROM maparesumo
    WHERE data BETWEEN $1 AND $2
    GROUP BY id_loja, data
),
resumo_caixa AS (
    SELECT 
        id_loja,
        data,
        SUM(vendabruta - cancelamento - desconto) AS venda_liquida
    FROM caixa
    WHERE data BETWEEN $1 AND $2
    GROUP BY id_loja, data
)
SELECT 
    m.id_loja,
    m.data,
    m.valor_venda,
    c.venda_liquida,
    (m.valor_venda - c.venda_liquida) AS diferenca
FROM resumo_mapas m
INNER JOIN resumo_caixa c
    ON m.id_loja = c.id_loja
   AND m.data = c.data
ORDER BY m.data, m.id_loja;
`;

async function executarMapaResumo(dataInicial, dataFinal) {
    let client;

    try {
        console.log(`\n[MAPA RESUMO] Período: ${dataInicial} até ${dataFinal}`);
        client = await pool.connect();

        const resultado = await client.query(queryMapaResumo, [dataInicial, dataFinal]);

        // Mantém o histórico do relatório no Supabase.
        const { error: deleteError } = await supabase
            .from('relatorio_mapa_resumo')
            .delete()
            .gte('data', dataInicial)
            .lte('data', dataFinal);

        if (deleteError) {
            throw new Error(`Erro ao limpar relatorio_mapa_resumo no período: ${deleteError.message}`);
        }

        if (resultado.rows.length > 0) {
            const { error: insertError } = await supabase
                .from('relatorio_mapa_resumo')
                .insert(resultado.rows);

            if (insertError) {
                throw new Error(`Erro ao inserir relatorio_mapa_resumo: ${insertError.message}`);
            }
        }

        console.log(`[MAPA RESUMO] ${resultado.rows.length} registro(s) processado(s).`);

        return {
            sucesso: true,
            total: resultado.rows.length,
            dados: resultado.rows,
            dataInicial,
            dataFinal,
            erro: null
        };
    } finally {
        if (client) client.release();
    }
}

app.post('/executar-mapa-resumo', async (req, res) => {
    const dataInicial = String(req.body?.dataInicial || '').trim();
    const dataFinal = String(req.body?.dataFinal || '').trim();

    console.log('\nNova solicitação: /executar-mapa-resumo');

    if (!validarIntervalo(dataInicial, dataFinal)) {
        return res.status(400).json({
            sucesso: false,
            total: 0,
            dados: [],
            erro: 'Intervalo de datas inválido. Informe datas YYYY-MM-DD e a inicial não pode ser maior que a final.'
        });
    }

    try {
        return res.json(await executarMapaResumo(dataInicial, dataFinal));
    } catch (erro) {
        console.error('[MAPA RESUMO] ERRO:', erro);
        return res.status(500).json({
            sucesso: false,
            total: 0,
            dados: [],
            dataInicial,
            dataFinal,
            erro: erro.message || 'Erro desconhecido.'
        });
    }
});

// ============================================================
// MÓDULO: POWERSHELL EMBUTIDO
// ============================================================
const POWERSHELL_EXE = 'C:\\Windows\\System32\\WindowsPowerShell\\v1.0\\powershell.exe';
const POWERSHELL_DIR = '\\\\10.0.0.33\\arquivos\\Financeiro\\Conciliações Contabilidade\\Conciliador Unisuper';

function encodeWebSocketText(text) {
    const payload = Buffer.from(String(text), 'utf8');
    const length = payload.length;

    if (length < 126) {
        return Buffer.concat([Buffer.from([0x81, length]), payload]);
    }

    if (length < 65536) {
        const header = Buffer.alloc(4);
        header[0] = 0x81;
        header[1] = 126;
        header.writeUInt16BE(length, 2);
        return Buffer.concat([header, payload]);
    }

    const header = Buffer.alloc(10);
    header[0] = 0x81;
    header[1] = 127;
    header.writeBigUInt64BE(BigInt(length), 2);
    return Buffer.concat([header, payload]);
}

function decodeWebSocketFrames(buffer) {
    const mensagens = [];
    let offset = 0;

    while (buffer.length - offset >= 2) {
        const byte1 = buffer[offset];
        const byte2 = buffer[offset + 1];

        const fin = (byte1 & 0x80) !== 0;
        const opcode = byte1 & 0x0f;
        const masked = (byte2 & 0x80) !== 0;
        let length = byte2 & 0x7f;
        let headerLength = 2;

        if (!fin) break;

        if (length === 126) {
            if (buffer.length - offset < 4) break;
            length = buffer.readUInt16BE(offset + 2);
            headerLength = 4;
        } else if (length === 127) {
            if (buffer.length - offset < 10) break;
            const bigLength = buffer.readBigUInt64BE(offset + 2);
            if (bigLength > BigInt(Number.MAX_SAFE_INTEGER)) break;
            length = Number(bigLength);
            headerLength = 10;
        }

        if (!masked) break;

        const total = headerLength + 4 + length;
        if (buffer.length - offset < total) break;

        const maskStart = offset + headerLength;
        const payloadStart = maskStart + 4;
        const mask = buffer.subarray(maskStart, maskStart + 4);
        const payload = Buffer.alloc(length);

        for (let i = 0; i < length; i++) {
            payload[i] = buffer[payloadStart + i] ^ mask[i % 4];
        }

        mensagens.push({ opcode, payload });
        offset += total;
    }

    return {
        mensagens,
        restante: buffer.subarray(offset)
    };
}

function enviarWebSocketClose(socket) {
    try {
        socket.write(Buffer.from([0x88, 0x00]));
    } catch (_) {}
}

const httpServer = http.createServer(app);

httpServer.on('upgrade', (request, socket) => {
    if (request.url !== '/terminal') {
        socket.destroy();
        return;
    }

    const key = request.headers['sec-websocket-key'];

    if (!key) {
        socket.destroy();
        return;
    }

    const accept = crypto
        .createHash('sha1')
        .update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11')
        .digest('base64');

    socket.write(
        'HTTP/1.1 101 Switching Protocols\r\n' +
        'Upgrade: websocket\r\n' +
        'Connection: Upgrade\r\n' +
        `Sec-WebSocket-Accept: ${accept}\r\n` +
        '\r\n'
    );

    console.log('[POWERSHELL] Terminal WebSocket conectado.');

    const ps = spawn(POWERSHELL_EXE, [
        '-NoLogo',
        '-NoProfile',
        '-NoExit',
        '-ExecutionPolicy', 'Bypass',
        '-Command',
        `Set-Location -LiteralPath '${POWERSHELL_DIR.replace(/'/g, "''")}'`
    ], {
        windowsHide: true,
        cwd: POWERSHELL_DIR,
        stdio: ['pipe', 'pipe', 'pipe']
    });

    let frameBuffer = Buffer.alloc(0);

    const enviarSaida = (dados) => {
        if (!socket.destroyed) {
            socket.write(encodeWebSocketText(dados.toString('utf8')));
        }
    };

    ps.stdout.on('data', enviarSaida);
    ps.stderr.on('data', enviarSaida);

    ps.on('error', (erro) => {
        enviarSaida(`\r\n[ERRO AO INICIAR POWERSHELL] ${erro.message}\r\n`);
        enviarWebSocketClose(socket);
        socket.end();
    });

    ps.on('close', (codigo) => {
        enviarSaida(`\r\n[PowerShell encerrado. Código: ${codigo}]\r\n`);
        if (!socket.destroyed) {
            enviarWebSocketClose(socket);
            socket.end();
        }
    });

    socket.on('data', (chunk) => {
        frameBuffer = Buffer.concat([frameBuffer, chunk]);

        const resultado = decodeWebSocketFrames(frameBuffer);
        frameBuffer = resultado.restante;

        for (const mensagem of resultado.mensagens) {
            if (mensagem.opcode === 0x8) {
                try { ps.kill(); } catch (_) {}
                enviarWebSocketClose(socket);
                socket.end();
                return;
            }

            if (mensagem.opcode === 0x9) {
                if (!socket.destroyed) socket.write(Buffer.concat([Buffer.from([0x8A, mensagem.payload.length]), mensagem.payload]));
                continue;
            }

            if (mensagem.opcode === 0x1) {
                if (!ps.stdin.destroyed) {
                    ps.stdin.write(mensagem.payload.toString('utf8'));
                }
            }
        }
    });

    socket.on('close', () => {
        try { ps.kill(); } catch (_) {}
        console.log('[POWERSHELL] Terminal desconectado.');
    });

    socket.on('error', () => {
        try { ps.kill(); } catch (_) {}
    });
});

// ============================================================
// SAÚDE DO SERVIDOR
// ============================================================
app.get('/health', (req, res) => {
    res.json({
        sucesso: true,
        servidor: 'conciliador_unificado',
        status: 'online',
        porta: PORT,
        modulos: {
            conciliador_fornecedores: true,
            conciliacao_fiscal_contabil: true,
            mapa_resumo: true,
            powershell_embutido: true
        }
    });
});

// ============================================================
// INICIALIZAÇÃO ÚNICA
// ============================================================
httpServer.listen(PORT, () => {
    console.log('');
    console.log('============================================================');
    console.log('        CONCILIADOR CONTÁBIL - SERVIDOR UNIFICADO');
    console.log('============================================================');
    console.log(`Servidor: http://localhost:${PORT}`);
    console.log(`Health:   http://localhost:${PORT}/health`);
    console.log('');
    console.log('Módulos disponíveis no mesmo processo:');
    console.log('  - Conciliador / Fornecedores');
    console.log('  - Conciliação Fiscal (contábil)');
    console.log('  - Mapa Resumo');
    console.log('  - PowerShell embutido');
    console.log('');
    console.log('Porta central: 5000');
    console.log('============================================================');
});

async function encerrarServidor() {
    console.log('\nEncerrando servidor...');

    httpServer.close(async () => {
        try {
            await pool.end();
        } catch (_) {}

        console.log('Servidor encerrado.');
        process.exit(0);
    });
}

process.on('SIGINT', encerrarServidor);
process.on('SIGTERM', encerrarServidor);