import https from 'https';
import { tableFromIPC } from '@apache-arrow/es2015-esm';

// shared global between invocation
let host = 'https://e2-dogfood.staging.cloud.databricks.com';
let statementsUrl = `${host}/api/2.0/sql/statements`;
let warehouseId = "77636a5b5b0e395a"; // Data Team Endpoint (Serverless medium)
const defaultStatements = "select 1000";


// our API design has GET with body, `fetch` does not like it
// https://github.com/bobbyhadz/aws-lambda-http-request-nodejs/blob/master/post-request.js
function getHistoryWithBody(authorization, payload) {
  const options = {
    hostname: 'e2-dogfood.staging.cloud.databricks.com',
    path: '/api/2.0/sql/history/queries',
    method: 'GET',
    port: 443,
    headers: {
      'Authorization': authorization,
      'Content-Type': 'application/json',
    },
  };
  
  return new Promise((resolve, reject) => {
    const req = https.request(options, res => {
      let rawData = '';

      res.on('data', chunk => {
        rawData += chunk;
      });

      res.on('end', () => {
        try {
          resolve(JSON.parse(rawData));
        } catch (err) {
          reject(new Error(err));
        }
      });
    });

    req.on('error', err => {
      reject(new Error(err));
    });

    req.write(payload);
    req.end();
  });
}

// fetchers
async function executeStatements(statements) {
    const headers = this.headers;
    return await fetch(statementsUrl, {
        headers,
        method: 'POST',
        body: JSON.stringify({
            "statement": statements,
            "warehouse_id": warehouseId,
            "wait_timeout": "0s",
            "disposition": "EXTERNAL_LINKS",
            "format": "ARROW_STREAM"
        }),
    }).then((response) => response.json())
}

async function getStatementResultByUrl(url) {
    const headers = this.headers;
    return await fetch(url, {
        headers,
        method: 'GET',
    }).then((response) => response.json());  
}

// utils
async function throttledGetStatementsResult(statementId) {
    return new Promise((res, err) => {
        const url = `${statementsUrl}/${statementId}`;
        getStatementResultByUrl.call(this, url).then((result) => {
            // wait for 500ms before next network request
            setTimeout(() => {
                res(result)
            }, 500);
        })
    })
}

async function getSucceededResult(statementId) {
    // cache the statement_id related to "statemnets" in this function scope
    // get looping until a succeed result is return
    // for SQLX: it is done by polling
    let succeededResult = undefined;
    while (true) {
        const currentResult = await throttledGetStatementsResult.call(this, statementId);
        if (currentResult.status.state === 'PENDING') {
            continue;
        } else if (currentResult.status.state === 'SUCCEEDED') {
            succeededResult = currentResult;
            break;
        } else {
            break;
        }
    }
    if (!succeededResult) {
        throw new Error("failed with result");
    }
    return succeededResult;
}

async function getNextChunkInfo(currentChunkInfo, statement_id) {
    // next link is actually the currentLinkInfo.next_chunk_internal_link
    const currentLinkInfo = currentChunkInfo.external_links[0];
    const {
        next_chunk_index,
        row_count,
        row_offset
    } = currentLinkInfo;
    if (next_chunk_index) {
        const nextOffset = row_offset + row_count; 
        const nextChunkUrl = `${statementsUrl}/${statement_id}/result/chunks/${next_chunk_index}?row_offset=${nextOffset}`;
        const nextChunkInfo = await getStatementResultByUrl.call(this,nextChunkUrl);
        return nextChunkInfo;   
    } else {
        return null;
    }
}

function buildPreview(table, rowNum) {
    const fields = table.schema.fields.map(f => `<th>${f.name}</th>`);
    let markdown = `<tr>${fields.join('')}</tr>`; // header row

    let rowIndex = 0;
    while ( rowIndex < rowNum) {
        // assume stringify
       const row = table.get(rowIndex);
       let tableRow = `<tr>`;
       for (let cell of row) {
            const [key, value] = cell;
            try {
                tableRow += `<td>${value.toString()}</td>`
            } catch {
                tableRow += `<td>...</td>`
            }
       }
       tableRow += `</tr>`;
       markdown += tableRow;
       rowIndex++; 
    }
    return markdown.toString();
}

async function getTablePreview(s3url) {
    let tablePreview;

    try {
        const table = await tableFromIPC(
            fetch(s3url)
        );
        const numOfRow = table.numRows;
        if (!numOfRow) {
            tablePreview = 'no data';
        } else {
            const previewNumOfRow = Math.min(numOfRow, 10);
            tablePreview = buildPreview(table, previewNumOfRow);
        }
    } catch (e) {
        tablePreview = 'error generating preview'
    }

    return tablePreview;
}


async function getRunStatementsResult(statements) {
    const result = await executeStatements.call(this, statements);
    let currentResult = await getSucceededResult.call(this, result.statement_id);
    let statement_id = currentResult.statement_id;
    
    let currentChunkInfo = currentResult.result;
    const s3Locations = [];
    let totalRowCount = 0;
    // we have manifest actually
    while (currentChunkInfo) {
        const currentLinkInfo = currentChunkInfo.external_links[0];
        const currentS3Location = currentLinkInfo.external_link;
        const currentExpiration =  currentLinkInfo.expiration;
        const currentRowCount = currentLinkInfo.row_count;
        s3Locations.push({
            url: currentS3Location,
            expiration: currentExpiration,
            row_count: currentRowCount
        });
        totalRowCount += currentRowCount;
        currentChunkInfo = await getNextChunkInfo.call(this, currentChunkInfo, statement_id);
    }

    const tablePreview = await getTablePreview(s3Locations[0].url)

    return {
        statement_id,
        statements,
        totalRowCount,
        s3Locations,
        tablePreview
    }
}

// handlers
async function handleRunStatements(body) {
    const statements = body.statements ?? defaultStatements;
    const result = await getRunStatementsResult.call(this, statements);
    const response = {
        statusCode: 200,
        body: JSON.stringify(result),
    };
    return response;
}

async function handleTablePreview(body) {
    const s3url = body.s3url;
    const tablePreview = await getTablePreview(s3url)
    return {
        statusCode: 200,
        body: JSON.stringify(tablePreview)
    }
}

async function handleMe() {
    const headers = this.headers;
    const url = `${host}/api/2.0/preview/scim/v2/Me`;
    const me = await fetch(url, {
        headers,
        method: 'GET',
    }).then((response) => response.json());

    return {
        statusCode: 200,
        body: JSON.stringify(me)
    }
}


// for now get the userId from the body
async function handleHistory(body) {
    const userId = body.userId;
    const authorization = this.headers.authorization;
    
    const payload = JSON.stringify({
        filter_by: {
            user_ids: [userId],
            warehouse_ids: [warehouseId]
        },
        "statuses": [
            "FINISHED",
            "FAILED"
        ],
        include_metrics: false,
        max_results: 5
    });
    
    const history = await getHistoryWithBody(authorization, payload)

    return {
        statusCode: 200,
        body: JSON.stringify(history)
    }
}


// serve html? maybe not
async function handleGet() {

}

export const handler = async(event) => {
    const {
        requestContext: {
            http: {
                method,
                path,
            }
        },
        headers: {
            authorization
        },
        body,
    } = event;

    if (!authorization) {
        return {
            statusCode: 401,
            body: 'Unauthorized',
        }
    }

    const context = {
        headers: {
            authorization,
            'Content-Type': 'application/json',
        }
    };

    if (method === 'POST') {
        if (path === '/run_statements') {
            return await handleRunStatements.call(context, JSON.parse(body))
        }
        if (path === '/table_preview') {
            return await handleTablePreview.call(context, JSON.parse(body));
        }
        if (path === '/me') {
            return await handleMe.call(context)
        }
        if (path === '/me/history') {
            return await handleHistory.call(context, JSON.parse(body))
        } else {
            return {
                statusCode: 400,
                body: 'Unsupported',
            }
        }
    } else {
        return {
            statusCode: 400,
            body: 'Unsupported',
        }
    }
};
