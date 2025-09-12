import {
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
	NodeOperationError,
	NodeConnectionType,
} from 'n8n-workflow';

import { BlockFrostAPI } from '@blockfrost/blockfrost-js';

export class CrowdBfExplorer implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Crowd BF Explorer',
		name: 'crowdBfExplorer',
		icon: 'file:crowd-bf.png',
		group: ['transform'],
		version: 1,
		subtitle: '={{$parameter["operation"]}}',
		description: 'Explore Cardano blockchain data via Blockfrost API',
		defaults: {
			name: 'Crowd BF Explorer',
		},
		inputs: [NodeConnectionType.Main],
		outputs: [NodeConnectionType.Main],
		credentials: [
			{
				name: 'blockfrostApi',
				required: true,
			},
		],
		properties: [
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				options: [
					{
						name: 'Get Address Info',
						value: 'getAddressInfo',
						description: 'Get address balance and info',
					},
					{
						name: 'Get Transaction',
						value: 'getTransaction',
						description: 'Get transaction details by hash',
					},
					{
						name: 'Get Recent Orders',
						value: 'getRecentOrders',
						description: 'Get recent Minswap/DEX orders for a wallet',
					},
				],
				default: 'getAddressInfo',
			},
			{
				displayName: 'Network',
				name: 'network',
				type: 'options',
				displayOptions: {
					show: {
						operation: ['getAddressInfo', 'getTransaction', 'getRecentOrders'],
					},
				},
				options: [
					{
						name: 'Mainnet',
						value: 'mainnet',
					},
					{
						name: 'Testnet',
						value: 'testnet',
					},
				],
				default: 'mainnet',
				description: 'Cardano network to use',
			},
			{
				displayName: 'Address',
				name: 'address',
				type: 'string',
				displayOptions: {
					show: {
						operation: ['getAddressInfo', 'getRecentOrders'],
					},
				},
				default: '',
				required: true,
				description: 'Cardano address to query',
				placeholder: 'addr1qxy...',
			},
			{
				displayName: 'Limit',
				name: 'limit',
				type: 'number',
				displayOptions: {
					show: {
						operation: ['getRecentOrders'],
					},
				},
				default: 20,
				description: 'Maximum number of transactions to check for orders',
				typeOptions: {
					minValue: 1,
					maxValue: 100,
				},
			},
			{
				displayName: 'Transaction Hash',
				name: 'transactionHash',
				type: 'string',
				displayOptions: {
					show: {
						operation: ['getTransaction'],
					},
				},
				default: '',
				required: true,
				description: 'Transaction hash to query',
				placeholder: 'abc123def456...',
			},
		],
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const items = this.getInputData();
		const returnData: INodeExecutionData[] = [];

		// Get credentials
		const credentials = await this.getCredentials('blockfrostApi');
		const blockfrostApiKey = credentials.apiKey as string;

		for (let i = 0; i < items.length; i++) {
			try {
				const operation = this.getNodeParameter('operation', i) as string;
				const network = this.getNodeParameter('network', i) as string;

				// Initialize Blockfrost API
				const blockfrostUrl = network === 'mainnet'
					? 'https://cardano-mainnet.blockfrost.io/api/v0'
					: 'https://cardano-testnet.blockfrost.io/api/v0';

				const blockfrost = new BlockFrostAPI({
					projectId: blockfrostApiKey,
					customBackend: blockfrostUrl,
				});

				if (operation === 'getAddressInfo') {
					const address = this.getNodeParameter('address', i) as string;

					try {
						// Get address info
						const addressInfo = await blockfrost.addresses(address);

						// Get address UTXOs
						const utxos = await blockfrost.addressesUtxos(address);

						// Get address transactions (last 10)
						const transactions = await blockfrost.addressesTransactions(address, {
							count: 10,
							order: 'desc'
						});

						returnData.push({
							json: {
								operation: 'getAddressInfo',
								network,
								address,
								balance: {
									ada: parseInt(addressInfo.amount[0].quantity) / 1000000,
									lovelace: parseInt(addressInfo.amount[0].quantity),
								},
								assets: addressInfo.amount.length > 1 ? addressInfo.amount.slice(1) : [],
								utxos: utxos.map((utxo: any) => ({
									txHash: utxo.tx_hash,
									outputIndex: utxo.output_index,
									amount: utxo.amount,
									dataHash: utxo.data_hash,
								})),
								recentTransactions: transactions.slice(0, 5),
								totalUtxos: utxos.length,
								totalTransactions: transactions.length,
								success: true,
							},
						});
					} catch (error: any) {
						if (error?.status_code === 404) {
							returnData.push({
								json: {
									operation: 'getAddressInfo',
									network,
									address,
									balance: {
										ada: 0,
										lovelace: 0,
									},
									assets: [],
									utxos: [],
									recentTransactions: [],
									totalTransactions: 0,
									success: true,
									note: 'Address not found on chain - likely unused address',
								},
							});
						} else {
							throw error;
						}
					}

				} else if (operation === 'getRecentOrders') {
					const address = this.getNodeParameter('address', i) as string;
					const limit = this.getNodeParameter('limit', i) as number;

					const orderHistory = await CrowdBfExplorer.getRecentOrdersByMetadata(blockfrost, address, limit);

					returnData.push({
						json: {
							operation: 'getRecentOrders',
							network,
							address,
							...orderHistory,
							success: true,
						},
					});

				} else if (operation === 'getTransaction') {
					const transactionHash = this.getNodeParameter('transactionHash', i) as string;

					// Get transaction details
					const transaction = await blockfrost.txs(transactionHash);

					// Get transaction UTXOs
					const utxos = await blockfrost.txsUtxos(transactionHash);

					// Get transaction metadata (if any)
					let metadata = null;
					try {
						metadata = await blockfrost.txsMetadata(transactionHash);
					} catch (error) {
						// No metadata - this is normal
					}

					returnData.push({
						json: {
							operation: 'getTransaction',
							network,
							transactionHash,
							transaction: {
								hash: transaction.hash,
								block: transaction.block,
								blockHeight: transaction.block_height,
								blockTime: transaction.block_time,
								slot: transaction.slot,
								index: transaction.index,
								fees: {
									ada: parseInt(transaction.fees) / 1000000,
									lovelace: parseInt(transaction.fees),
								},
								size: transaction.size,
								invalidBefore: transaction.invalid_before,
								invalidHereafter: transaction.invalid_hereafter,
								validContract: transaction.valid_contract,
							},
							inputs: utxos.inputs.map((input: any) => ({
								address: input.address,
								amount: input.amount,
								txHash: input.tx_hash,
								outputIndex: input.output_index,
								dataHash: input.data_hash,
								collateral: input.collateral,
							})),
							outputs: utxos.outputs.map((output: any) => ({
								address: output.address,
								amount: output.amount,
								outputIndex: output.output_index,
								dataHash: output.data_hash,
							})),
							metadata: metadata || null,
							success: true,
						},
					});
				}

			} catch (error: any) {
				const errorMessage = error instanceof Error ? error.message : 'Unknown error occurred';

				if (this.continueOnFail()) {
					returnData.push({
						json: {
							success: false,
							error: errorMessage,
							statusCode: error?.status_code || null,
							operation: this.getNodeParameter('operation', i),
							network: this.getNodeParameter('network', i),
						},
					});
				} else {
					throw new NodeOperationError(this.getNode(), `Cardano operation failed: ${errorMessage}`);
				}
			}
		}

		return [returnData];
	}

	/**
	 * Get recent DEX/Minswap orders by analyzing transaction metadata
	 */
	private static async getRecentOrdersByMetadata(
		blockfrost: BlockFrostAPI,
		walletAddress: string,
		limit: number
	) {
		// Get wallet transactions
		const transactions = await blockfrost.addressesTransactions(
			walletAddress,
			{ count: limit, order: 'desc' }
		);

		const orders = [];
		let processedCount = 0;

		for (const tx of transactions) {
			try {
				processedCount++;

				// Get transaction details
				const txDetails = await blockfrost.txs(tx.tx_hash);

				// Try to get metadata - if it fails, skip this transaction
				try {
					const metadata = await blockfrost.txsMetadata(tx.tx_hash);
					if (metadata && metadata.length > 0) {
						// Check for DEX/Minswap metadata
						const dexInfo = CrowdBfExplorer.detectDexMetadata(metadata);

						if (dexInfo.isDexTransaction) {
							// Get transaction UTXOs for more details
							const txUtxos = await blockfrost.txsUtxos(tx.tx_hash);

							orders.push({
								txHash: tx.tx_hash,
								timestamp: new Date(txDetails.block_time * 1000),
								fee: {
									ada: parseInt(txDetails.fees) / 1000000,
									lovelace: parseInt(txDetails.fees),
								},
								size: txDetails.size,
								block: txDetails.block,
								blockHeight: txDetails.block_height,
								metadata: metadata,
								dexDetection: dexInfo,
								orderType: CrowdBfExplorer.classifyOrderType(txUtxos, metadata),
								inputs: txUtxos.inputs.map((input: any) => ({
									address: input.address,
									amount: input.amount,
									txHash: input.tx_hash,
									outputIndex: input.output_index,
								})),
								outputs: txUtxos.outputs.map((output: any) => ({
									address: output.address,
									amount: output.amount,
									outputIndex: output.output_index,
								})),
							});
						}
					}
				} catch (metadataError) {
					// No metadata or error getting metadata - skip this transaction
					continue;
				}

				// Add a small delay to avoid hitting rate limits
				if (processedCount % 10 === 0) {
					await new Promise<void>((resolve) => setTimeout(resolve, 100));
				}

			} catch (error: any) {
				// Skip transactions that can't be processed
				continue;
			}
		}

		return {
			totalOrders: orders.length,
			transactionsScanned: processedCount,
			orders: orders,
			walletAddress: walletAddress,
			queryTime: new Date().toISOString(),
			note: 'Orders detected via transaction metadata analysis'
		};
	}

	/**
	 * Detect DEX-related metadata patterns
	 */
	private static detectDexMetadata(metadata: any[]): { isDexTransaction: boolean; detectionReason: string; platform?: string } {
		for (const entry of metadata) {
			const label = entry.label;
			const jsonMetadata = entry.json_metadata;

			// Check for CIP-20 messages (label 674)
			if (label === '674' && jsonMetadata?.msg) {
				const message = Array.isArray(jsonMetadata.msg)
					? jsonMetadata.msg.join(' ')
					: jsonMetadata.msg;

				if (typeof message === 'string') {
					const messageLower = message.toLowerCase();

					if (messageLower.includes('minswap')) {
						return {
							isDexTransaction: true,
							detectionReason: 'CIP-20 message mentions Minswap',
							platform: 'Minswap'
						};
					}

					if (messageLower.includes('swap') || messageLower.includes('dex')) {
						return {
							isDexTransaction: true,
							detectionReason: 'CIP-20 message mentions swap/DEX',
							platform: 'Unknown DEX'
						};
					}

					if (messageLower.includes('liquidity') || messageLower.includes('pool')) {
						return {
							isDexTransaction: true,
							detectionReason: 'CIP-20 message mentions liquidity/pool',
							platform: 'Unknown DEX'
						};
					}
				}
			}

			// Check for common DEX metadata labels
			if (label === '1337') {
				return {
					isDexTransaction: true,
					detectionReason: 'Uses metadata label 1337 (common DEX label)',
					platform: 'Unknown DEX'
				};
			}

			// Check for any metadata containing DEX keywords
			if (jsonMetadata && typeof jsonMetadata === 'object') {
				const metadataStr = JSON.stringify(jsonMetadata).toLowerCase();

				if (metadataStr.includes('minswap')) {
					return {
						isDexTransaction: true,
						detectionReason: 'Metadata contains Minswap reference',
						platform: 'Minswap'
					};
				}

				if (metadataStr.includes('sundaeswap')) {
					return {
						isDexTransaction: true,
						detectionReason: 'Metadata contains SundaeSwap reference',
						platform: 'SundaeSwap'
					};
				}

				if (metadataStr.includes('muesliswap')) {
					return {
						isDexTransaction: true,
						detectionReason: 'Metadata contains MuesliSwap reference',
						platform: 'MuesliSwap'
					};
				}

				if (metadataStr.includes('liquidit') || metadataStr.includes('pool') || metadataStr.includes('swap')) {
					return {
						isDexTransaction: true,
						detectionReason: 'Metadata contains DEX-related keywords',
						platform: 'Unknown DEX'
					};
				}
			}
		}

		return { isDexTransaction: false, detectionReason: 'No DEX metadata patterns detected' };
	}

	/**
	 * Classify the type of DEX order based on UTXOs and metadata
	 */
	private static classifyOrderType(txUtxos: any, metadata: any[]): string {
		// Analyze asset flow patterns
		const inputAssets = txUtxos.inputs.flatMap((input: any) => input.amount);
		const outputAssets = txUtxos.outputs.flatMap((output: any) => output.amount);

		const inputTokenCount = inputAssets.filter((asset: any) => asset.unit !== 'lovelace').length;
		const outputTokenCount = outputAssets.filter((asset: any) => asset.unit !== 'lovelace').length;

		// Check metadata for explicit operation type
		for (const entry of metadata) {
			if (entry.json_metadata?.msg) {
				const message = Array.isArray(entry.json_metadata.msg)
					? entry.json_metadata.msg.join(' ').toLowerCase()
					: entry.json_metadata.msg.toLowerCase();

				if (message.includes('swap')) return 'swap';
				if (message.includes('add liquidity') || message.includes('deposit')) return 'add_liquidity';
				if (message.includes('remove liquidity') || message.includes('withdraw')) return 'remove_liquidity';
				if (message.includes('harvest') || message.includes('reward')) return 'harvest_rewards';
			}
		}

		// Heuristics based on asset patterns
		if (inputTokenCount === 1 && outputTokenCount === 1) {
			// Single token in, different single token out = likely swap
			const inputToken = inputAssets.find((asset: any) => asset.unit !== 'lovelace');
			const outputToken = outputAssets.find((asset: any) => asset.unit !== 'lovelace');

			if (inputToken && outputToken && inputToken.unit !== outputToken.unit) {
				return 'swap';
			}
		} else if (inputTokenCount === 2 && outputTokenCount === 1) {
			return 'add_liquidity';
		} else if (inputTokenCount === 1 && outputTokenCount === 2) {
			return 'remove_liquidity';
		} else if (inputTokenCount === 0 && outputTokenCount > 0) {
			return 'harvest_rewards';
		}

		return 'unknown_dex_operation';
	}
}