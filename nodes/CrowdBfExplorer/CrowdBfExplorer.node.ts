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
				],
				default: 'getAddressInfo',
			},
			{
				displayName: 'Network',
				name: 'network',
				type: 'options',
				displayOptions: {
					show: {
						operation: ['getAddressInfo', 'getTransaction', 'getRecentOrders', 'parseTradingHistory'],
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
/**
 * Parse trading history from raw order data - handles DEX aggregation
 */
private static parseTradingHistory(orderHistory: any, walletAddress: string) {
	const orders = [...(orderHistory?.orders || [])].sort((a: any, b: any) =>
		new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime()
	);

	const pending: any[] = [];
	const trades: any[] = [];
	const processedOrders = new Set<string>(); // Track processed order IDs

	console.log(`Processing ${orders.length} orders for wallet ${walletAddress}`);

	for (const order of orders) {
		if (processedOrders.has(order.txHash)) continue;

		const { msg, extra } = CrowdBfExplorer.parseCIP674(order);
		const lowerMsg = msg.map((s: any) => String(s).toLowerCase());

		const isAgg = lowerMsg.some((s: string) => s.includes('aggregator market order'));
		const isExec = lowerMsg.some((s: string) => s.includes('order executed'));
		const isDexhunterTrade = lowerMsg.some((s: string) => s.includes('dexhunter trade'));
		const isSteelSwap = lowerMsg.some((s: string) => s.includes('steelswap'));
		const isOrderPlacement = (isDexhunterTrade || isSteelSwap) && !isExec && !isAgg;

		console.log(`Order ${order.txHash}: isAgg=${isAgg}, isExec=${isExec}, isDexhunter=${isDexhunterTrade}, isSteelSwap=${isSteelSwap}, isPlacement=${isOrderPlacement}`);

		// Handle aggregator orders (existing logic)
		if (isAgg && extra) {
			console.log(`Processing aggregator order: ${order.txHash}`);
			const opt = CrowdBfExplorer.extractOrderOptions(extra);
			if (opt?.amountIn && opt?.assetIn && opt?.assetOut) {
				pending.push({
					ts: new Date(order.timestamp).getTime(),
					txid: order.txHash,
					assetIn: opt.assetIn,
					assetOut: opt.assetOut,
					amountIn: opt.amountIn,
					executions: [],
					isComplete: false,
				});
				console.log(`Added aggregator order to pending. Total pending: ${pending.length}`);
			}
			continue;
		}

		// Handle direct order placements (NEW)
		if (isOrderPlacement) {
			console.log(`Processing order placement: ${order.txHash}, platform: ${order.dexDetection?.platform}`);
			const orderIntent = CrowdBfExplorer.extractOrderIntent(order, walletAddress);
			console.log(`Order intent extracted:`, orderIntent);
			if (orderIntent) {
				pending.push({
					ts: new Date(order.timestamp).getTime(),
					txid: order.txHash,
					assetIn: orderIntent.assetIn,
					assetOut: orderIntent.assetOut,
					amountIn: orderIntent.amountIn,
					orderType: orderIntent.orderType,
					executions: [],
					isComplete: false,
				});
				console.log(`Added to pending orders. Total pending: ${pending.length}`);
			}
			continue;
		}

		// Handle order executions
		if (isExec) {
			console.log(`Processing execution: ${order.txHash}, looking for matching pending orders`);
			const ts = new Date(order.timestamp).getTime();
			const candidates = pending
				.filter((p: any) => !p.isComplete && ts - p.ts >= 0 && ts - p.ts <= 10 * 60 * 1000)
				.sort((a: any, b: any) => b.ts - a.ts);

			console.log(`Found ${candidates.length} candidate pending orders for execution`);

			if (candidates.length > 0) {
				// Found matching placement order
				const delivered = CrowdBfExplorer.tokensReceived(order, walletAddress, undefined);
				let matchingOrder = candidates[0];

				// Try to match by expected output asset
				if (delivered) {
					const byUnit = candidates.find((p: any) =>
						CrowdBfExplorer.sameUnit(p.assetOut, delivered.unit)
					);
					if (byUnit) matchingOrder = byUnit;
				}

				console.log(`Matched execution ${order.txHash} with order ${matchingOrder.txid}`);

				// Add this execution to the order
				matchingOrder.executions.push({
					txHash: order.txHash,
					platform: order.dexDetection?.platform || 'Unknown',
					timestamp: order.timestamp,
					delivered: delivered,
					order: order
				});

				// Check if this order is complete (heuristic: wait 2 minutes after last execution)
				const isComplete = CrowdBfExplorer.isOrderComplete(matchingOrder, ts);
				if (isComplete) {
					matchingOrder.isComplete = true;
					const trade = CrowdBfExplorer.processFinalTrade(matchingOrder, walletAddress);
					if (trade) {
						trades.push(trade);
						console.log(`Completed trade created for order ${matchingOrder.txid}`);
					}
				}
			} else {
				// Orphaned execution - no matching placement found
				console.log(`Orphaned execution detected: ${order.txHash}`);
				const orphanedTrade = CrowdBfExplorer.processOrphanedExecution(order, walletAddress);
				if (orphanedTrade) {
					trades.push(orphanedTrade);
					console.log(`Orphaned trade created: ${order.txHash}`);
				}
			}

			processedOrders.add(order.txHash);
			continue;
		}

		// Handle other DEX transactions that don't fit the placement/execution pattern
		if (!isAgg && !isExec && !isOrderPlacement) {
			console.log(`Processing generic DEX transaction: ${order.txHash}`);
			// Check if this looks like a standalone trade (SteelSwap, etc.)
			const standaloneTrade = CrowdBfExplorer.processGenericDexTransaction(order, walletAddress);
			if (standaloneTrade) {
				trades.push(standaloneTrade);
				console.log(`Standalone trade created: ${order.txHash}`);
			}
			processedOrders.add(order.txHash);
		}
	}

	// Process any remaining incomplete orders (timeout-based completion)
	console.log(`Processing remaining ${pending.length} pending orders`);
	const now = Date.now();
	for (const pendingOrder of pending) {
		if (!pendingOrder.isComplete) {
			if (pendingOrder.executions.length > 0) {
				// Has executions - check timeout
				const lastExecTime = Math.max(...pendingOrder.executions.map((e: any) =>
					new Date(e.timestamp).getTime()
				));

				// If last execution was more than 5 minutes ago, consider it complete
				if (now - lastExecTime > 5 * 60 * 1000) {
					const trade = CrowdBfExplorer.processFinalTrade(pendingOrder, walletAddress);
					if (trade) {
						trades.push(trade);
						console.log(`Timeout-completed trade: ${pendingOrder.txid}`);
					}
				}
			} else {
				// No executions - this might be a standalone trade (like Dexhunter)
				// Check if order is old enough to be considered complete
				const orderAge = now - pendingOrder.ts;
				if (orderAge > 2 * 60 * 1000) { // 2 minutes old
					const standaloneTrade = CrowdBfExplorer.processStandaloneTrade(pendingOrder, walletAddress);
					if (standaloneTrade) {
						trades.push(standaloneTrade);
						console.log(`Standalone pending trade: ${pendingOrder.txid}`);
					}
				}
			}
		}
	}

	console.log(`Final result: ${trades.length} trades found`);

	return {
		trades,
		summary: {
			totalTrades: trades.length,
			buyTrades: trades.filter(t => t.direction === 'BUY').length,
			sellTrades: trades.filter(t => t.direction === 'SELL').length,
			ordersScanned: orders.length,
			transactionsScanned: orderHistory.transactionsScanned,
			aggregatedOrders: trades.filter(t => t.executionCount > 1).length,
		}
	};
}

/**
 * Extract order intent from placement transaction
 */
private static extractOrderIntent(order: any, walletAddress: string) {
	const userInputs = order.inputs?.filter((input: any) => input.address === walletAddress) || [];
	const userOutputs = order.outputs?.filter((output: any) => output.address === walletAddress) || [];

	if (userInputs.length === 0) return null;

	// Calculate what user is sending vs receiving
	const assetsIn = userInputs.flatMap((input: any) => input.amount || []);
	const assetsOut = userOutputs.flatMap((output: any) => output.amount || []);

	const tokensIn = assetsIn.filter((asset: any) => asset.unit !== 'lovelace');
	const tokensOut = assetsOut.filter((asset: any) => asset.unit !== 'lovelace');

	const adaIn = assetsIn
		.filter((asset: any) => asset.unit === 'lovelace')
		.reduce((sum: number, asset: any) => sum + parseInt(asset.quantity || '0'), 0);

	const adaOut = assetsOut
		.filter((asset: any) => asset.unit === 'lovelace')
		.reduce((sum: number, asset: any) => sum + parseInt(asset.quantity || '0'), 0);

	console.log(`Order ${order.txHash}: ADA in=${adaIn}, ADA out=${adaOut}, tokens in=${tokensIn.length}, tokens out=${tokensOut.length}`);

	// Determine order type and assets
	if (tokensIn.length > 0 && tokensOut.length === 0) {
		// User is sending tokens and not receiving any back - likely a SELL order
		const primaryToken = tokensIn[0];
		return {
			assetIn: primaryToken.unit,
			assetOut: 'lovelace',
			amountIn: primaryToken.quantity,
			orderType: 'sell'
		};
	} else if (tokensIn.length === 0 && adaIn > adaOut + 1000000) { // 1 ADA threshold for fees
		// User is sending more ADA than receiving - likely a BUY order
		const netAda = adaIn - adaOut;
		return {
			assetIn: 'lovelace',
			assetOut: 'unknown', // Will be determined from execution
			amountIn: String(netAda),
			orderType: 'buy'
		};
	} else if (tokensIn.length > 0 && tokensOut.length > 0) {
		// User sending and receiving tokens - could be a swap
		const primaryTokenIn = tokensIn[0];
		const primaryTokenOut = tokensOut[0];

		if (primaryTokenIn.unit !== primaryTokenOut.unit) {
			return {
				assetIn: primaryTokenIn.unit,
				assetOut: primaryTokenOut.unit,
				amountIn: primaryTokenIn.quantity,
				orderType: 'swap'
			};
		}
	}

	return null;
}

/**
 * Check if an order is complete based on execution timing
 */
private static isOrderComplete(order: any, currentTime: number): boolean {
	if (order.executions.length === 0) return false;

	const lastExecTime = Math.max(...order.executions.map((e: any) =>
		new Date(e.timestamp).getTime()
	));

	// Consider order complete if no new executions for 2 minutes
	return currentTime - lastExecTime > 2 * 60 * 1000;
}

/**
 * Process final trade from aggregated executions
 */
private static processFinalTrade(order: any, walletAddress: string) {
	if (order.executions.length === 0) return null;

	// Aggregate all deliveries across executions
	const totalDelivered = order.executions.reduce((total: any, exec: any) => {
		if (exec.delivered) {
			if (!total.unit) {
				total.unit = exec.delivered.unit;
				total.qty = 0;
			}
			if (CrowdBfExplorer.sameUnit(total.unit, exec.delivered.unit)) {
				total.qty += exec.delivered.qty;
			}
		}
		return total;
	}, { unit: null, qty: 0 });

	// Calculate total ADA received (for SELL orders)
	let totalAdaReceived = 0;
	if (order.orderType === 'sell') {
		for (const exec of order.executions) {
			for (const output of (exec.order.outputs || [])) {
				if (output.address === walletAddress) {
					for (const amount of (output.amount || [])) {
						if (amount.unit === 'lovelace') {
							totalAdaReceived += parseInt(amount.quantity || '0');
						}
					}
				}
			}
		}
	}

	// Build final trade object
	const platforms = [...new Set(order.executions.map((e: any) => e.platform))];
	const firstExecution = order.executions[0];

	if (order.assetIn === 'lovelace' && totalDelivered.unit && totalDelivered.qty > 0) {
		// BUY trade
		const adaIn = CrowdBfExplorer.toAda(order.amountIn);
		const tokenOut = totalDelivered.qty;

		if (adaIn && Number.isFinite(tokenOut) && tokenOut > 0) {
			const identifier = CrowdBfExplorer.normalizeIdentifier(totalDelivered.unit);
			const assetName = CrowdBfExplorer.decodeAssetNameFromIdentifier(identifier);
			const rawPrice = adaIn / tokenOut;

			return {
				timestamp: firstExecution.timestamp,
				direction: "BUY",
				assetName,
				identifier,
				amountIn: CrowdBfExplorer.formatNum6(adaIn),
				amountOut: CrowdBfExplorer.formatNum6(tokenOut),
				price: CrowdBfExplorer.formatPrice6(rawPrice),
				txHash: order.txid,
				executionCount: order.executions.length,
				platforms: platforms.join(', '),
				executionHashes: order.executions.map((e: any) => e.txHash),
			};
		}
	} else if (order.assetIn !== 'lovelace' && totalAdaReceived > 0) {
		// SELL trade
		const tokenIn = Number(order.amountIn);
		const adaOut = CrowdBfExplorer.toAda(String(totalAdaReceived));

		if (adaOut && Number.isFinite(tokenIn) && tokenIn > 0) {
			const identifier = CrowdBfExplorer.normalizeIdentifier(order.assetIn);
			const assetName = CrowdBfExplorer.decodeAssetNameFromIdentifier(identifier);
			const rawPrice = adaOut / tokenIn;

			return {
				timestamp: firstExecution.timestamp,
				direction: "SELL",
				assetName,
				identifier,
				amountIn: CrowdBfExplorer.formatNum6(tokenIn),
				amountOut: CrowdBfExplorer.formatNum6(adaOut),
				price: CrowdBfExplorer.formatPrice6(rawPrice),
				txHash: order.txid,
				executionCount: order.executions.length,
				platforms: platforms.join(', '),
				executionHashes: order.executions.map((e: any) => e.txHash),
			};
		}
	}

	return null;
}

/**
 * Process standalone trade (like Dexhunter) that doesn't have separate execution transactions
 */
private static processStandaloneTrade(order: any, _walletAddress: string) {
	// For standalone trades, we need to analyze the order transaction itself
	// to determine what was traded

	if (!order.txid) return null;

	// This would need the actual order transaction data
	// For now, we'll create a placeholder that shows the order was detected
	// but couldn't be fully parsed

	return {
		timestamp: new Date(order.ts).toISOString(),
		direction: "UNKNOWN",
		assetName: "Unknown Asset",
		identifier: order.assetOut || "unknown",
		amountIn: order.amountIn ? CrowdBfExplorer.formatNum6(Number(order.amountIn) / 1e6) : "0.000000",
		amountOut: "0.000000",
		price: "0.000000",
		txHash: order.txid,
		executionCount: 0,
		platforms: 'Dexhunter',
		executionHashes: [],
		note: "Standalone trade detected but full parsing not implemented"
	};
}

/**
 * Process orphaned execution (execution without matching placement order)
 */
private static processOrphanedExecution(order: any, walletAddress: string) {
	const delivered = CrowdBfExplorer.tokensReceived(order, walletAddress, undefined);

	if (!delivered) return null;

	// For orphaned executions, we need to infer the input from the transaction
	// Look at what was likely sent to get this delivery
	let adaSpent = 0;

	// Check for ADA that went to DEX/pool addresses (not back to user)
	for (const output of (order.outputs || [])) {
		if (output.address !== walletAddress) {
			for (const amount of (output.amount || [])) {
				if (amount.unit === 'lovelace') {
					adaSpent += parseInt(amount.quantity || '0');
				}
			}
		}
	}

	if (adaSpent > 0) {
		const adaIn = CrowdBfExplorer.toAda(String(adaSpent));
		const tokenOut = delivered.qty;

		if (adaIn && Number.isFinite(tokenOut) && tokenOut > 0) {
			const identifier = CrowdBfExplorer.normalizeIdentifier(delivered.unit);
			const assetName = CrowdBfExplorer.decodeAssetNameFromIdentifier(identifier);
			const rawPrice = adaIn / tokenOut;

			return {
				timestamp: order.timestamp,
				direction: "BUY",
				assetName,
				identifier,
				amountIn: CrowdBfExplorer.formatNum6(adaIn),
				amountOut: CrowdBfExplorer.formatNum6(tokenOut),
				price: CrowdBfExplorer.formatPrice6(rawPrice),
				txHash: order.txHash,
				executionCount: 1,
				platforms: order.dexDetection?.platform || 'Unknown',
				executionHashes: [order.txHash],
				note: "Orphaned execution - placement order not found in scan window"
			};
		}
	}

	return null;
}

/**
 * Process generic DEX transaction (SteelSwap, etc.)
 */
private static processGenericDexTransaction(order: any, walletAddress: string) {
	const userInputs = order.inputs?.filter((input: any) => input.address === walletAddress) || [];
	const userOutputs = order.outputs?.filter((output: any) => output.address === walletAddress) || [];

	if (userInputs.length === 0 || userOutputs.length === 0) return null;

	// Calculate net flows
	const assetsIn = userInputs.flatMap((input: any) => input.amount || []);
	const assetsOut = userOutputs.flatMap((output: any) => output.amount || []);

	const adaIn = assetsIn
		.filter((asset: any) => asset.unit === 'lovelace')
		.reduce((sum: number, asset: any) => sum + parseInt(asset.quantity || '0'), 0);

	const adaOut = assetsOut
		.filter((asset: any) => asset.unit === 'lovelace')
		.reduce((sum: number, asset: any) => sum + parseInt(asset.quantity || '0'), 0);

	const netAda = adaOut - adaIn; // Positive if user received ADA, negative if user sent ADA

	// If user received net ADA, this was likely a SELL
	if (netAda > 1000000) { // Received more than 1 ADA
		const adaReceived = CrowdBfExplorer.toAda(String(netAda));

		return {
			timestamp: order.timestamp,
			direction: "SELL",
			assetName: "Unknown Token",
			identifier: "unknown",
			amountIn: "0.000000",
			amountOut: CrowdBfExplorer.formatNum6(adaReceived || 0),
			price: "0.000000",
			txHash: order.txHash,
			executionCount: 1,
			platforms: order.dexDetection?.platform || 'Unknown DEX',
			executionHashes: [order.txHash],
			note: "Generic DEX transaction - full parsing not implemented"
		};
	}

	// If user sent net ADA, this was likely a BUY
	if (netAda < -1000000) { // Sent more than 1 ADA
		const adaSent = CrowdBfExplorer.toAda(String(-netAda));

		return {
			timestamp: order.timestamp,
			direction: "BUY",
			assetName: "Unknown Token",
			identifier: "unknown",
			amountIn: CrowdBfExplorer.formatNum6(adaSent || 0),
			amountOut: "0.000000",
			price: "0.000000",
			txHash: order.txHash,
			executionCount: 1,
			platforms: order.dexDetection?.platform || 'Unknown DEX',
			executionHashes: [order.txHash],
			note: "Generic DEX transaction - full parsing not implemented"
		};
	}

	return null;
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