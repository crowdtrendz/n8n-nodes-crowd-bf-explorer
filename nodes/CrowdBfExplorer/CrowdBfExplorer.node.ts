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
		description: 'Explore Cardano blockchain data and submit transactions via Blockfrost API',
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
						name: 'Submit Transaction',
						value: 'submitTransaction',
						description: 'Submit a signed CBOR transaction to the blockchain',
					},
				],
				default: 'getAddressInfo',
			},
			{
				displayName: 'Network',
				name: 'network',
				type: 'options',
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
				default: 'testnet',
				description: 'Cardano network to use',
			},
			{
				displayName: 'Address',
				name: 'address',
				type: 'string',
				displayOptions: {
					show: {
						operation: ['getAddressInfo'],
					},
				},
				default: '',
				required: true,
				description: 'Cardano address to query',
				placeholder: 'addr1qxy...',
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
			{
				displayName: 'Transaction CBOR',
				name: 'transactionCbor',
				type: 'string',
				displayOptions: {
					show: {
						operation: ['submitTransaction'],
					},
				},
				default: '',
				required: true,
				description: 'Signed transaction in CBOR hex format',
				placeholder: '84a400818258...',
				typeOptions: {
					rows: 4,
				},
			},
			{
				displayName: 'Additional Options',
				name: 'additionalOptions',
				type: 'collection',
				displayOptions: {
					show: {
						operation: ['submitTransaction'],
					},
				},
				default: {},
				placeholder: 'Add Option',
				options: [
					{
						displayName: 'Validate Only',
						name: 'validateOnly',
						type: 'boolean',
						default: false,
						description: 'Whether to only validate the transaction without submitting it',
					},
					{
						displayName: 'Wait for Confirmation',
						name: 'waitForConfirmation',
						type: 'boolean',
						default: false,
						description: 'Whether to wait for transaction confirmation before returning',
					},
					{
						displayName: 'Confirmation Timeout (seconds)',
						name: 'confirmationTimeout',
						type: 'number',
						default: 300,
						description: 'Maximum time to wait for confirmation in seconds',
						displayOptions: {
							show: {
								waitForConfirmation: [true],
							},
						},
					},
				],
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
								utxos: utxos.map(utxo => ({
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
							inputs: utxos.inputs.map(input => ({
								address: input.address,
								amount: input.amount,
								txHash: input.tx_hash,
								outputIndex: input.output_index,
								dataHash: input.data_hash,
								collateral: input.collateral,
							})),
							outputs: utxos.outputs.map(output => ({
								address: output.address,
								amount: output.amount,
								outputIndex: output.output_index,
								dataHash: output.data_hash,
							})),
							metadata: metadata || null,
							success: true,
						},
					});

				} else if (operation === 'submitTransaction') {
					const transactionCbor = this.getNodeParameter('transactionCbor', i) as string;
					const additionalOptions = this.getNodeParameter('additionalOptions', i, {}) as {
						validateOnly?: boolean;
						waitForConfirmation?: boolean;
						confirmationTimeout?: number;
					};

					// Validate CBOR format
					if (!CrowdBfExplorer.isValidCborHex(transactionCbor)) {
						throw new NodeOperationError(
							this.getNode(), 
							'Invalid CBOR hex format. Transaction CBOR must be a valid hexadecimal string.'
						);
					}

					// Clean the CBOR string (remove whitespace and ensure proper format)
					const cleanCbor = transactionCbor.replace(/\s+/g, '').toLowerCase();

					if (additionalOptions.validateOnly) {
						// Only validate the transaction
						try {
							// Blockfrost doesn't have a dedicated validate endpoint, 
							// so we'll do basic CBOR validation here
							const isValid = CrowdBfExplorer.validateCborStructure(cleanCbor);
							
							returnData.push({
								json: {
									operation: 'submitTransaction',
									network,
									transactionCbor: cleanCbor,
									validation: {
										isValid,
										cborLength: cleanCbor.length,
										estimatedSize: cleanCbor.length / 2, // bytes
									},
									success: true,
									note: 'Validation only - transaction not submitted',
								},
							});
						} catch (error: any) {
							throw new NodeOperationError(
								this.getNode(),
								`CBOR validation failed: ${error.message}`
							);
						}
					} else {
						// Submit the transaction using Blockfrost SDK
						try {
							// Convert hex string to buffer for submission
							const cborBuffer = Buffer.from(cleanCbor, 'hex');
							
							// Use Blockfrost SDK for submission - this handles HTTP details internally
							const txHash = await blockfrost.txSubmit(cborBuffer);

							let confirmationData = null;

							if (additionalOptions.waitForConfirmation) {
								// Wait for transaction confirmation
								const timeout = additionalOptions.confirmationTimeout || 300;
								confirmationData = await CrowdBfExplorer.waitForConfirmation(blockfrost, txHash, timeout);
							}

							returnData.push({
								json: {
									operation: 'submitTransaction',
									network,
									transactionCbor: cleanCbor,
									submission: {
										transactionHash: txHash,
										submittedAt: new Date().toISOString(),
										cborSize: cleanCbor.length / 2, // bytes
									},
									confirmation: confirmationData,
									success: true,
								},
							});

						} catch (error: any) {
							// Handle specific Blockfrost submission errors
							if (error?.status_code === 400) {
								const errorMessage = error?.response?.message || error?.message || 'Transaction submission failed';
								throw new NodeOperationError(
									this.getNode(),
									`Transaction rejected by network: ${errorMessage}. Please check your transaction CBOR and ensure it's properly signed.`
								);
							} else if (error?.status_code === 405) {
								throw new NodeOperationError(
									this.getNode(),
									'Method not allowed. This may indicate an issue with the Blockfrost API endpoint or the transaction format.'
								);
							} else if (error?.status_code === 413) {
								throw new NodeOperationError(
									this.getNode(),
									'Transaction too large. The CBOR transaction exceeds the maximum allowed size.'
								);
							} else if (error?.status_code === 403) {
								throw new NodeOperationError(
									this.getNode(),
									'Access forbidden. Please check your Blockfrost API key and permissions.'
								);
							} else if (error?.status_code === 429) {
								throw new NodeOperationError(
									this.getNode(),
									'Rate limit exceeded. Please wait and try again later.'
								);
							} else {
								// Generic error handling
								const errorMessage = error?.response?.message || error?.message || 'Transaction submission failed';
								throw new NodeOperationError(
									this.getNode(),
									`Transaction submission failed: ${errorMessage}`
								);
							}
						}
					}
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
	 * Validate if string is valid hexadecimal CBOR format
	 */
	private static isValidCborHex(cbor: string): boolean {
		const cleanCbor = cbor.replace(/\s+/g, '');
		
		// Check if it's valid hex
		if (!/^[0-9a-fA-F]+$/.test(cleanCbor)) {
			return false;
		}
		
		// Check if length is even (hex pairs)
		if (cleanCbor.length % 2 !== 0) {
			return false;
		}

		// Minimum reasonable length for a Cardano transaction
		if (cleanCbor.length < 100) {
			return false;
		}

		return true;
	}

	/**
	 * Basic CBOR structure validation for Cardano transactions
	 */
	private static validateCborStructure(cbor: string): boolean {
		try {
			// Basic checks for Cardano transaction CBOR structure
			// Cardano transactions typically start with specific CBOR prefixes
			
			// Check for CBOR array indicator (transactions are CBOR arrays)
			const firstByte = parseInt(cbor.substring(0, 2), 16);
			
			// CBOR major type 4 (array) with various lengths
			const isArray = (firstByte & 0xE0) === 0x80;
			
			if (!isArray) {
				throw new Error('CBOR does not appear to be an array structure');
			}

			return true;
		} catch (error) {
			return false;
		}
	}

	/**
	 * Wait for transaction confirmation on the blockchain
	 */
	private static async waitForConfirmation(
		blockfrost: BlockFrostAPI, 
		txHash: string, 
		timeoutSeconds: number
	): Promise<any> {
		const startTime = Date.now();
		const timeout = timeoutSeconds * 1000;
		const pollInterval = 10000; // 10 seconds

		while (Date.now() - startTime < timeout) {
			try {
				const transaction = await blockfrost.txs(txHash);
				
				if (transaction && transaction.block) {
					// Transaction is confirmed
					return {
						confirmed: true,
						confirmationTime: new Date().toISOString(),
						blockHash: transaction.block,
						blockHeight: transaction.block_height,
						slot: transaction.slot,
						waitedSeconds: Math.round((Date.now() - startTime) / 1000),
					};
				}
			} catch (error: any) {
				// Transaction not yet available, continue waiting
				if (error?.status_code !== 404) {
					// Unexpected error
					throw error;
				}
			}

			// Wait before next poll
			await new Promise(resolve => setTimeout(resolve, pollInterval));
		}

		// Timeout reached
		return {
			confirmed: false,
			timedOut: true,
			waitedSeconds: timeoutSeconds,
			note: 'Transaction submission successful but confirmation timeout reached. Transaction may still be processing.',
		};
	}
}