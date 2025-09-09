import {
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
	NodeOperationError,
	NodeConnectionType,
} from 'n8n-workflow';

import { BlockFrostAPI } from '@blockfrost/blockfrost-js';

interface BlockfrostAssetResponse {
	policy_id: string;
	asset_name: string;
	fingerprint: string;
	quantity: string;
	initial_mint_tx_hash: string;
	mint_or_burn_count: number;
	onchain_metadata: any;
	metadata: any;
}

interface BlockfrostAddressResponse {
	address: string;
	quantity: string;
}

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
					{
						name: 'Get DEX History',
						value: 'parseTradingHistory',
						description: 'Parse recent orders into clean trading history with prices',
					},
					{
						name: 'Get Latest DEX Trade',
						value: 'getLatestDexTrade',
						description: 'Get the most recent DEX trade from last 10 transactions',
					},
					{
						name: 'Resolve ADA Handle',
						value: 'resolveAdaHandle',
						description: 'Resolve ADA handle to blockchain address',
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
						operation: ['getAddressInfo', 'getTransaction', 'getRecentOrders', 'parseTradingHistory', 'resolveAdaHandle'],
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
						operation: ['getAddressInfo', 'getRecentOrders', 'parseTradingHistory'],
					},
				},
				default: '',
				required: true,
				description: 'Cardano address to query',
				placeholder: 'addr1qxy...',
			},
			{
				displayName: 'Handle Name',
				name: 'handleName',
				type: 'string',
				displayOptions: {
					show: {
						operation: ['resolveAdaHandle'],
					},
				},
				default: '',
				required: true,
				placeholder: '$spitzer or spitzer',
				description: 'The ADA handle name to resolve (with or without $ prefix, e.g., "$spitzer" or "spitzer")',
			},
			{
				displayName: 'Include Metadata',
				name: 'includeMetadata',
				type: 'boolean',
				displayOptions: {
					show: {
						operation: ['resolveAdaHandle'],
					},
				},
				default: false,
				description: 'Whether to include additional metadata in the response',
			},
			{
				displayName: 'Limit',
				name: 'limit',
				type: 'number',
				displayOptions: {
					show: {
						operation: ['getRecentOrders', 'parseTradingHistory'],
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

		// Special handling for getLatestDexTrade - process all items at once
		if (items.length > 0) {
			const operation = this.getNodeParameter('operation', 0) as string;

			if (operation === 'getLatestDexTrade') {
				// Process all input items to find the latest trade
				const allTrades = items.map(item => item.json).filter(trade => trade && trade.timestamp);

				if (allTrades.length > 0) {
					const latestTrade = allTrades.reduce((latest, current) =>
						new Date(current.timestamp as string) > new Date(latest.timestamp as string) ? current : latest
					);

					returnData.push({
						json: {
							...latestTrade,
							operation: 'getLatestDexTrade',
						},
					});
				} else {
					returnData.push({
						json: {
							operation: 'getLatestDexTrade',
							success: false,
							error: 'No valid trade data found in input. This operation should be used after Get DEX History.',
							note: 'Connect this node after Get DEX History to process trade data.',
						},
					});
				}

				return [returnData];
			}
		}

		// Get credentials
		const credentials = await this.getCredentials('blockfrostApi');
		const blockfrostApiKey = credentials.apiKey as string;

		for (let i = 0; i < items.length; i++) {
			try {
				const operation = this.getNodeParameter('operation', i) as string;
				const network = this.getNodeParameter('network', i) as string;

				if (operation === 'resolveAdaHandle') {
					const handleName = this.getNodeParameter('handleName', i) as string;
					const includeMetadata = this.getNodeParameter('includeMetadata', i) as boolean;

					try {
						const handleResult = await CrowdBfExplorer.resolveAdaHandle(
							blockfrostApiKey,
							handleName,
							network,
							includeMetadata
						);

						returnData.push({
							json: {
								operation: 'resolveAdaHandle',
								network,
								...handleResult,
								success: true,
							},
						});
					} catch (error: any) {
						returnData.push({
							json: {
								operation: 'resolveAdaHandle',
								network,
								handleName,
								success: false,
								error: error.message,
								cardanoAddress: null,
							},
						});
					}
					continue;
				}

				// Initialize Blockfrost API for other operations
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

				} else if (operation === 'parseTradingHistory') {
					const address = this.getNodeParameter('address', i) as string;
					const limit = this.getNodeParameter('limit', i) as number;

					// First get the raw orders
					const orderHistory = await CrowdBfExplorer.getRecentOrdersByMetadata(blockfrost, address, limit);

					// Then parse them into clean trading history
					const tradingHistory = CrowdBfExplorer.parseTradingHistory(orderHistory, address);

					// Return each trade as a separate item for easier processing
					if (tradingHistory.trades && tradingHistory.trades.length > 0) {
						for (const trade of tradingHistory.trades) {
							returnData.push({
								json: {
									operation: 'getDEXHistory',
									network,
									...trade,
									success: true,
								},
							});
						}
					} else {
						// No trades found - return summary
						returnData.push({
							json: {
								operation: 'getDEXHistory',
								network,
								address,
								tradesFound: 0,
								transactionsScanned: orderHistory.transactionsScanned,
								ordersDetected: orderHistory.totalOrders,
								success: true,
								note: 'No completed trades found in the scanned transactions',
							},
						});
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
	 * Resolve ADA handle to blockchain address using Blockfrost
	 */
	private static async resolveAdaHandle(
		blockfrostApiKey: string,
		handleName: string,
		network: string = 'mainnet',
		includeMetadata: boolean = false
	) {
		if (!blockfrostApiKey) {
			throw new Error('Blockfrost API key is required');
		}

		// Clean the handle name - remove $ prefix if present
		let cleanHandleName = handleName.trim();
		if (cleanHandleName.startsWith('$')) {
			cleanHandleName = cleanHandleName.substring(1);
		}

		// First try to find the handle by searching wallet assets
		// This is more reliable than trying to guess encoding
		try {
			const result = await CrowdBfExplorer.findHandleByWalletAssets(blockfrostApiKey, cleanHandleName, network, includeMetadata);
			if (result) return result;
		} catch (error) {
			// Continue to direct resolution if wallet search fails
		}

		// Fallback to direct asset resolution with multiple approaches
		const isSubHandle = cleanHandleName.includes('@');

		if (isSubHandle) {
			return await CrowdBfExplorer.resolveSubHandle(blockfrostApiKey, cleanHandleName, network, includeMetadata);
		} else {
			return await CrowdBfExplorer.resolveRegularHandle(blockfrostApiKey, cleanHandleName, network, includeMetadata);
		}
	}

	/**
	 * Find handle by searching wallet assets - more reliable method
	 */
	private static async findHandleByWalletAssets(
		blockfrostApiKey: string,
		handleName: string,
		network: string,
		includeMetadata: boolean
	) {
		// This approach searches for the handle by checking assets with the known policy ID
		// We'll get assets from the policy and decode them to find matching handles

		const policyId = 'f0ff48bbb7bbe9d59a40f1ce90e9e9d0ff5002ec48f232b49ca0fb9a';

		try {
			// Get assets from the ADA Handle policy
			const policyUrl = network === 'mainnet'
				? `https://cardano-mainnet.blockfrost.io/api/v0/assets/policy/${policyId}`
				: `https://cardano-testnet.blockfrost.io/api/v0/assets/policy/${policyId}`;

			// We'll try to search through paginated results
			for (let page = 1; page <= 5; page++) { // Limit to 5 pages to avoid timeout
				const response = await fetch(`${policyUrl}?page=${page}&count=100`, {
					headers: {
						'project_id': blockfrostApiKey,
					},
				});

				if (!response.ok) {
					if (page === 1) throw new Error(`Failed to fetch policy assets: ${response.status}`);
					break; // Stop if we can't get more pages
				}

				const assets = await response.json();
				if (!Array.isArray(assets) || assets.length === 0) break;

				// Check each asset to see if it matches our handle
				for (const asset of assets) {
					try {
						const assetName = asset.asset || asset.asset_name || asset;
						if (typeof assetName === 'string' && assetName.startsWith(policyId)) {
							const hexPart = assetName.substring(56); // Remove policy ID
							const decodedName = CrowdBfExplorer.hexToString(hexPart);

							if (decodedName === handleName) {
								// Found a match! Now get the address
								const result = await CrowdBfExplorer.tryResolveAsset(
									blockfrostApiKey,
									assetName,
									handleName,
									'found_in_policy',
									network,
									includeMetadata
								);
								if (result) return result;
							}
						}
					} catch (error) {
						// Skip assets that can't be decoded
						continue;
					}
				}

				// Add delay between pages to avoid rate limiting
				await new Promise(resolve => setTimeout(resolve, 100));
			}

			return null; // Handle not found in policy assets
		} catch (error) {
			throw new Error(`Failed to search policy assets: ${(error as Error).message}`);
		}
	}

	/**
	 * Convert hex string to readable string
	 */
	private static hexToString(hex: string): string {
		try {
			const buffer = Buffer.from(hex, 'hex');
			return buffer.toString('utf8');
		} catch {
			return '';
		}
	}

	/**
	 * Resolve regular ADA handle (no @ symbol)
	 */
	private static async resolveRegularHandle(
		blockfrostApiKey: string,
		cleanHandleName: string,
		network: string,
		includeMetadata: boolean
	) {
		// Try multiple encoding approaches and policy IDs for regular handles
		const encodingAttempts = [
			{
				name: 'utf8',
				hex: Buffer.from(cleanHandleName, 'utf8').toString('hex')
			},
			{
				name: 'ascii',
				hex: Buffer.from(cleanHandleName, 'ascii').toString('hex')
			},
			{
				name: 'latin1',
				hex: Buffer.from(cleanHandleName, 'latin1').toString('hex')
			},
			{
				name: 'hex_direct',
				hex: cleanHandleName // Try if it's already hex
			}
		];

		// Try multiple policy IDs in case there are different versions
		const policyIds = [
			'f0ff48bbb7bbe9d59a40f1ce90e9e9d0ff5002ec48f232b49ca0fb9a', // Standard ADA Handle policy
			'000de140000de1404000de1404000de1404000de1404000de1404000', // Alternative policy (if exists)
		];

		for (const policyId of policyIds) {
			for (const encoding of encodingAttempts) {
				try {
					const assetId = `${policyId}${encoding.hex}`;
					const result = await CrowdBfExplorer.tryResolveAsset(blockfrostApiKey, assetId, cleanHandleName, encoding.name, network, includeMetadata);
					if (result) return result;
				} catch (error) {
					continue;
				}
			}
		}

		throw new Error(`Regular handle not found with any encoding method. Tried: ${encodingAttempts.map(e =>
			`${e.name} (${e.hex})`
		).join(', ')}. Handle: ${cleanHandleName}`);
	}

	/**
	 * Resolve subHandle (contains @ symbol)
	 */
	private static async resolveSubHandle(
		blockfrostApiKey: string,
		cleanHandleName: string,
		network: string,
		includeMetadata: boolean
	) {
		// For subHandles, try multiple encoding approaches
		const encodingAttempts = [
			{
				name: 'utf8',
				hex: Buffer.from(cleanHandleName, 'utf8').toString('hex')
			},
			{
				name: 'ascii',
				hex: Buffer.from(cleanHandleName, 'ascii').toString('hex')
			},
			{
				name: 'parts_utf8',
				hex: CrowdBfExplorer.encodeSubHandleParts(cleanHandleName, 'utf8')
			},
			{
				name: 'parts_ascii',
				hex: CrowdBfExplorer.encodeSubHandleParts(cleanHandleName, 'ascii')
			}
		];

		// Try the standard policy ID and potential subHandle-specific policies
		const policyIds = [
			'f0ff48bbb7bbe9d59a40f1ce90e9e9d0ff5002ec48f232b49ca0fb9a', // Standard ADA Handle policy
			// Add subHandle-specific policy IDs here when known
		];

		for (const policyId of policyIds) {
			for (const encoding of encodingAttempts) {
				if (!encoding.hex) continue;

				try {
					const assetId = `${policyId}${encoding.hex}`;
					const result = await CrowdBfExplorer.tryResolveAsset(blockfrostApiKey, assetId, cleanHandleName, encoding.name, network, includeMetadata);
					if (result) {
						result.isSubHandle = true;
						return result;
					}
				} catch (error) {
					continue;
				}
			}
		}

		throw new Error(`SubHandle not found with any encoding method. Tried: ${encodingAttempts.filter(e => e.hex).map(e =>
			`${e.name} (${e.hex})`
		).join(', ')}. SubHandle: ${cleanHandleName}. Note: SubHandles may use different policy IDs not yet discovered.`);
	}

	/**
	 * Try encoding subHandle parts separately
	 */
	private static encodeSubHandleParts(subHandle: string, encoding: BufferEncoding): string | null {
		try {
			const parts = subHandle.split('@');
			if (parts.length !== 2) return null;

			const [name, domain] = parts;
			// Try different combinations for subHandle encoding
			const combinations = [
				`${Buffer.from(name, encoding).toString('hex')}40${Buffer.from(domain, encoding).toString('hex')}`, // @ = 40 in hex
				`${Buffer.from(name, encoding).toString('hex')}2d${Buffer.from(domain, encoding).toString('hex')}`, // Try dash (2d)
				`${Buffer.from(name, encoding).toString('hex')}5f${Buffer.from(domain, encoding).toString('hex')}`, // Try underscore (5f)
				`${Buffer.from(name, encoding).toString('hex')}2e${Buffer.from(domain, encoding).toString('hex')}`, // Try period (2e)
			];

			return combinations[0]; // Return the first attempt (@ symbol)
		} catch {
			return null;
		}
	}

	/**
	 * Try to resolve a specific asset ID
	 */
	private static async tryResolveAsset(
		blockfrostApiKey: string,
		assetId: string,
		handleName: string,
		encoding: string,
		network: string,
		includeMetadata: boolean
	) {
		const debugInfo = {
			originalHandle: handleName,
			encoding: encoding,
			handleHex: assetId.length > 56 ? assetId.substring(56) : assetId,
			assetId,
			policyId: assetId.length > 56 ? assetId.substring(0, 56) : 'unknown'
		};

		try {
			// Blockfrost endpoint for the specific asset
			const blockfrostUrl = network === 'mainnet'
				? `https://cardano-mainnet.blockfrost.io/api/v0/assets/${assetId}`
				: `https://cardano-testnet.blockfrost.io/api/v0/assets/${assetId}`;

			const response = await fetch(blockfrostUrl, {
				headers: {
					'project_id': blockfrostApiKey,
				},
			});

			if (response.ok) {
				const assetData = await response.json() as BlockfrostAssetResponse;

				// Get the asset addresses to find the current holder(s)
				const addressesUrl = network === 'mainnet'
					? `https://cardano-mainnet.blockfrost.io/api/v0/assets/${assetId}/addresses`
					: `https://cardano-testnet.blockfrost.io/api/v0/assets/${assetId}/addresses`;

				const addressesResponse = await fetch(addressesUrl, {
					headers: {
						'project_id': blockfrostApiKey,
					},
				});

				if (addressesResponse.ok) {
					const addressesData = await addressesResponse.json() as BlockfrostAddressResponse[];

					if (addressesData.length > 0) {
						// Find the address that actually holds the handle (quantity > 0)
						const activeHolders = addressesData.filter(addr =>
							parseInt(addr.quantity) > 0
						);

						if (activeHolders.length === 0) {
							return null; // No active holders, try next approach
						}

						// Get the primary holder (should be only one for a handle)
						const holderAddress = activeHolders[0].address;
						const holderQuantity = activeHolders[0].quantity;

						const result: any = {
							handleName,
							cleanHandleName: handleName,
							cardanoAddress: holderAddress,
							status: 'success',
							resolvedWith: encoding,
						};

						// If there are multiple active holders, include this info
						if (activeHolders.length > 1) {
							result.multipleHolders = true;
							result.allHolders = activeHolders.map(holder => ({
								address: holder.address,
								quantity: holder.quantity
							}));
							result.note = `Handle has ${activeHolders.length} active holders. Primary holder returned.`;
						}

						if (includeMetadata) {
							result.metadata = {
								handleName,
								cleanHandleName: handleName,
								assetId,
								policyId: assetData.policy_id,
								fingerprint: assetData.fingerprint,
								quantity: assetData.quantity,
								holderQuantity: holderQuantity,
								totalHolders: addressesData.length,
								activeHolders: activeHolders.length,
								initialMintTxHash: assetData.initial_mint_tx_hash,
								mintOrBurnCount: assetData.mint_or_burn_count,
								onchainMetadata: assetData.onchain_metadata,
								resolvedAt: new Date().toISOString(),
								network,
								debug: debugInfo
							};
						}

						return result;
					}
				}
			}
		} catch (error) {
			// Continue to next attempt
		}

		return null; // Asset not found or not accessible
	}

	/**
	 * Parse trading history from raw order data
	 */
	private static parseTradingHistory(orderHistory: any, walletAddress: string) {
		const orders = [...(orderHistory?.orders || [])].sort((a: any, b: any) =>
			new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime()
		);

		const pending: any[] = [];
		const trades: any[] = [];

		for (const order of orders) {
			const { msg, extra } = CrowdBfExplorer.parseCIP674(order);
			const lowerMsg = msg.map((s: any) => String(s).toLowerCase());
			const isAgg = lowerMsg.some((s: string) => s.includes('aggregator market order'));
			const isExec = lowerMsg.some((s: string) => s.includes('order executed'));

			if (isAgg && extra) {
				const opt = CrowdBfExplorer.extractOrderOptions(extra);
				if (opt?.amountIn && opt?.assetIn && opt?.assetOut) {
					pending.push({
						ts: new Date(order.timestamp).getTime(),
						txid: order.txHash,
						assetIn: opt.assetIn,
						assetOut: opt.assetOut,
						amountIn: opt.amountIn,
					});
				}
				continue;
			}

			if (isExec) {
				const ts = new Date(order.timestamp).getTime();
				const candidates = pending
					.filter((p: any) => ts - p.ts >= 0 && ts - p.ts <= 10 * 60 * 1000)
					.sort((a: any, b: any) => b.ts - a.ts);

				if (!candidates.length) continue;

				const delivered = CrowdBfExplorer.tokensReceived(order, walletAddress, undefined);
				let pair = candidates[0];

				if (delivered) {
					const byUnit = candidates.find((p: any) => CrowdBfExplorer.sameUnit(p.assetOut, delivered.unit));
					if (byUnit) pair = byUnit;
				}

				const idx = pending.indexOf(pair);
				if (idx >= 0) pending.splice(idx, 1);

				// BUY (ADA -> token)
				if (pair.assetIn === 'lovelace' && delivered) {
					const adaIn = CrowdBfExplorer.toAda(pair.amountIn);
					const tokenOut = Number(delivered.qty);

					if (adaIn != null && Number.isFinite(tokenOut) && tokenOut > 0) {
						const identifier = CrowdBfExplorer.normalizeIdentifier(delivered.unit);
						const assetName = CrowdBfExplorer.decodeAssetNameFromIdentifier(identifier);
						const rawPrice = adaIn / tokenOut; // ADA per token

						trades.push({
							timestamp: order.timestamp,
							direction: "BUY",
							assetName,
							identifier,
							amountIn: CrowdBfExplorer.formatNum6(adaIn),
							amountOut: CrowdBfExplorer.formatNum6(tokenOut),
							price: CrowdBfExplorer.formatPrice6(rawPrice),
							txHash: order.txHash,
						});
					}
					continue;
				}

				// SELL (token -> ADA)
				if (pair.assetIn !== 'lovelace') {
					let lovelaceToWallet = 0;
					for (const o of (order.outputs || [])) {
						if (o?.address === walletAddress) {
							for (const a of (o.amount || [])) {
								if (a.unit === 'lovelace') {
									lovelaceToWallet += Number(a.quantity || '0');
								}
							}
						}
					}

					const adaOut = CrowdBfExplorer.toAda(String(lovelaceToWallet));
					const tokenIn = Number(pair.amountIn);

					if (adaOut != null && Number.isFinite(tokenIn) && tokenIn > 0) {
						const identifier = CrowdBfExplorer.normalizeIdentifier(pair.assetIn);
						const assetName = CrowdBfExplorer.decodeAssetNameFromIdentifier(identifier);
						const rawPrice = adaOut / tokenIn; // ADA per token

						trades.push({
							timestamp: order.timestamp,
							direction: "SELL",
							assetName,
							identifier,
							amountIn: CrowdBfExplorer.formatNum6(tokenIn),
							amountOut: CrowdBfExplorer.formatNum6(adaOut),
							price: CrowdBfExplorer.formatPrice6(rawPrice),
							txHash: order.txHash,
						});
					}
				}
			}
		}

		return {
			trades,
			summary: {
				totalTrades: trades.length,
				buyTrades: trades.filter(t => t.direction === 'BUY').length,
				sellTrades: trades.filter(t => t.direction === 'SELL').length,
				ordersScanned: orders.length,
				transactionsScanned: orderHistory.transactionsScanned,
			}
		};
	}

	/**
	 * Parse CIP-674 metadata
	 */
	private static parseCIP674(order: any) {
		const row = (order?.metadata || []).find((m: any) => m?.label === '674' && m?.json_metadata);
		if (!row) return { msg: [], extra: null };

		const msg = Array.isArray(row.json_metadata?.msg) ? row.json_metadata.msg : [];
		let extra = null;
		const ed = row.json_metadata?.extraData;

		if (Array.isArray(ed) && ed.length) {
			try {
				extra = JSON.parse(ed.join(''));
			} catch {
				// Invalid JSON in extraData
			}
		}

		return { msg, extra };
	}

	/**
	 * Extract order options from CIP-674 extra data
	 */
	private static extractOrderOptions(extra: any) {
		if (!extra?.orderOptions?.length) return null;
		const opt = extra.orderOptions[0];
		const big = (k: string) => opt?.[k]?.$bigint ?? null;

		return {
			assetIn: opt?.assetIn?.$asset || opt?.assetIn?.asset || opt?.assetIn?.unit || null,
			assetOut: opt?.assetOut?.$asset || opt?.assetOut?.asset || opt?.assetOut?.unit || null,
			amountIn: big('amountIn'),
		};
	}

	/**
	 * Check if two units are the same (handle different formats)
	 */
	private static sameUnit(u1: string, u2: string): boolean {
		return !!u1 && !!u2 && (u1 === u2 || u1.replace('.', '') === u2.replace('.', ''));
	}

	/**
	 * Find tokens received by wallet in transaction outputs
	 */
	private static tokensReceived(order: any, walletAddr: string, expectedUnit?: string) {
		for (const o of (order.outputs || [])) {
			if (o?.address !== walletAddr) continue;
			for (const a of (o.amount || [])) {
				if (a.unit !== 'lovelace' && (!expectedUnit || CrowdBfExplorer.sameUnit(a.unit, expectedUnit))) {
					return { unit: a.unit, qty: Number(a.quantity) };
				}
			}
		}
		return null;
	}

	/**
	 * Normalize asset identifier format
	 */
	private static normalizeIdentifier(unit: string): string {
		if (!unit || unit === 'lovelace') return 'lovelace';
		if (unit.includes('.')) return unit;
		if (unit.length > 56) {
			const policy = unit.slice(0, 56);
			const assetHex = unit.slice(56);
			return `${policy}.${assetHex}`;
		}
		return unit;
	}

	/**
	 * Decode asset name from identifier
	 */
	private static decodeAssetNameFromIdentifier(identifier: string): string {
		if (identifier === 'lovelace') return 'ADA';
		const parts = identifier.split('.');
		if (parts.length !== 2) return identifier;
		const assetHex = parts[1];

		try {
			const buf = Buffer.from(assetHex, 'hex');
			const ascii = buf.toString('utf8');
			if (/^[\x20-\x7E]+$/.test(ascii)) return ascii;
		} catch {
			// Failed to decode
		}

		return identifier;
	}

	/**
	 * Convert lovelace to ADA
	 */
	private static toAda(lovelaceStr: string): number | null {
		const n = Number(lovelaceStr);
		return Number.isFinite(n) ? n / 1e6 : null;
	}

	/**
	 * Format number to 6 decimal places
	 */
	private static formatNum6(n: number): string {
		return Number(n).toFixed(6);
	}

	/**
	 * Format price to 6 decimal places
	 */
	private static formatPrice6(p: number): string {
		return Number(p).toFixed(6);
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