import { ESTransporter } from 'searchkit'
import type { SearchRequest } from "searchkit"
import { SignatureV4 } from '@aws-sdk/signature-v4';
import Client from "@searchkit/api";
import { Sha256 } from '@aws-crypto/sha256-js';
import { HttpRequest } from '@aws-sdk/protocol-http';
import { defaultProvider } from '@aws-sdk/credential-provider-node';

const endpoint = "<OPENSEARCH_ENDPOINT>";

class MyTransporter extends ESTransporter {
  async performNetworkRequest(requests: SearchRequest[]) {
	const signer = new SignatureV4({
		credentials: defaultProvider(),
		service: 'aoss',
		region: 'us-east-1',
		sha256: Sha256,
	  });

	  const url = new URL(endpoint);

	  const request = new HttpRequest({
		hostname: url.hostname,
		path: url.pathname,
		body: this.createElasticsearchQueryFromRequest(requests),
		method: 'POST',
		headers: {
		  'Content-Type': 'application/json',
		  host: url.hostname,
		},
	  });
	
	// sign the request and extract the signed headers, body and method
	const { headers, body, method } = await signer.sign(request);

	return await fetch(endpoint, {
		headers,
		body,
		method,
	  })
  }
}

async function handleOptions(request: Request) {
	return new Response(null, {
		headers: {
		"Access-Control-Allow-Origin": "*",
		"Access-Control-Allow-Methods": "GET, POST, OPTIONS",
		"Access-Control-Allow-Headers": "Content-Type",
		},
	});
}

const client = Client({
		connection: new MyTransporter({},{ debug: true }),  
		search_settings: {
			search_attributes: ['name', 'description', 'type.keyword'],
			result_attributes: ['name', 'description', 'type', 'score'],
			highlight_attributes: ['description'],
			facet_attributes: ['type.keyword']
		}
	},
	{
		debug: true
})

async function executeRequest(request: Request) {
	const body = await request.json();
		
	return client.handleRequest(body, {
		hooks: {
			beforeSearch: (searchRequests) => {
			  const uiRequest = searchRequests[0]   
			  return [
				{
				  ...uiRequest,
				  body: {
					...uiRequest.body,
		
					query: {
						function_score: {
							query: { ...uiRequest.body.query },
							functions: [
								{ 
									filter: { bool: { must: { match: { type: { query: "ecommerce"}}}}},
									script_score: { script: "_score * (5 + (0.17 * doc['score'].value))" }
								}
							]
							
						}
					}
				  }
				}
			  ]
			  
			},
		}
	});
}

export default {
	async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
		if (request.method === "OPTIONS") {
			// Handle CORS preflight requests
			return handleOptions(request);
		}
		
		const results = await executeRequest(request);
		
		return new Response(JSON.stringify(results), {
			headers: {
			"content-type": "application/json",
			"Access-Control-Allow-Origin": "*",
			"Access-Control-Allow-Methods": "GET, HEAD, POST, OPTIONS",
			"Access-Control-Allow-Headers": "Content-Type",
			},
		});
	},
};
