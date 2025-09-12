import {
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
	NodeOperationError,
	NodeConnectionType,
} from 'n8n-workflow';

import { BlockFrostAPI } from '@blockfrost/blockfrost-js';

export class DEXTrade implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'DEX Trade Parser',
		name: 'dexTrade',
		icon: 'file:dex-trade.png',
		group: ['transform'],
		version: 1,
		subtitle: '={{$parameter["operation"]}}',
		description: 'Parse DEX trading history from Cardano blockchain',
		defaults: {
			name: 'DEX Trade Parser',
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
						name: 'Parse Trading History',
						value: 'parseTradingHistory',
						description: 'Parse DEX orders into clean trading history with prices',
					},
					{
						name: 'Get Latest DEX Trade',
						value: 'getLatestDexTrade',
						description: 'Get the most recent DEX trade from input data',
					},
				],
				default: 'parseTradingHistory',
			},
			{
				displayName: 'Network',
				name: 'network',
				type: 'options',
				displayOptions: {
					show: {
						operation: ['parseTradingHistory'],
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
				displayName: 'Wallet Address',
				name: 'address',
				type: 'string',
				displayOptions: {
					show: {
						operation: ['parseTradingHistory'],
					},
				},
				default: '',
				required: true,
				description: 'Cardano wallet address to analyze',
				placeholder: 'addr1qxy...',
			},
		],
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const items = this.getInputData();
		const returnData: INodeExecutionData[] = [];
		const operation = this.getNodeParameter('operation', 0) as string;

		if (operation === 'getLatestDexTrade') {
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
						error: 'No valid trade data found in input.',
						note: 'Connect this node after Parse Trading History to process trade data.',
					},
				});
			}
			return [returnData];
		}

		// For parseTradingHistory operation
		if (operation === 'parseTradingHistory') {
			const credentials = await this.getCredentials('blockfrostApi');
			const blockfrostApiKey = credentials.apiKey as string;
			const network = this.getNodeParameter('network', 0) as string;
			const blockfrostUrl = network === 'mainnet'
				? 'https://cardano-mainnet.blockfrost.io/api/v0'
				: 'https://cardano-testnet.blockfrost.io/api/v0';

			const blockfrost = new BlockFrostAPI({
				projectId: blockfrostApiKey,
				customBackend: blockfrostUrl,
			});

			for (let i = 0; i < items.length; i++) {
				try {
					const inputData = items[i].json;
					let walletAddress: string;
					let orders: any[];
					let processableOrderHistory: any;

					// Handle different input formats
					if (inputData.orders && Array.isArray(inputData.orders)) {
						orders = inputData.orders as any[];
						walletAddress = inputData.walletAddress as string;
						processableOrderHistory = inputData;
					} else if (Array.isArray(inputData)) {
						orders = inputData;
						walletAddress = this.getNodeParameter('address', i) as string;
						processableOrderHistory = {
							orders: orders,
							walletAddress: walletAddress,
							transactionsScanned: orders.length,
							totalOrders: orders.length
						};
					} else if (inputData.operation === 'getRecentOrders' && inputData.orders) {
						orders = inputData.orders as any[];
						walletAddress = (inputData.walletAddress || inputData.address || this.getNodeParameter('address', i)) as string;
						processableOrderHistory = {
							orders: orders,
							walletAddress: walletAddress,
							transactionsScanned: (inputData.transactionsScanned as number) || orders.length,
							totalOrders: (inputData.totalOrders as number) || orders.length
						};
					} else {
						const possibleOrders = inputData.orders || inputData.data || inputData.transactions;
						if (possibleOrders && Array.isArray(possibleOrders)) {
							orders = possibleOrders as any[];
							walletAddress = (inputData.walletAddress || inputData.address || this.getNodeParameter('address', i)) as string;
							processableOrderHistory = {
								orders: orders,
								walletAddress: walletAddress,
								transactionsScanned: (inputData.transactionsScanned as number) || orders.length,
								totalOrders: (inputData.totalOrders as number) || orders.length
							};
						} else {
							returnData.push({
								json: {
									operation: 'parseTradingHistory',
									success: false,
									error: 'Invalid input format. Expected order history with orders array.',
									note: 'Connect after Get Recent Orders or provide data with proper structure',
									receivedKeys: Object.keys(inputData).join(', '),
									sampleData: JSON.stringify(inputData).substring(0, 200) + '...'
								},
							});
							continue;
						}
					}

					if (!walletAddress) {
						returnData.push({
							json: {
								operation: 'parseTradingHistory',
								success: false,
								error: 'Wallet address not found. Please specify address parameter.',
							},
						});
						continue;
					}

					if (!orders || orders.length === 0) {
						returnData.push({
							json: {
								operation: 'parseTradingHistory',
								network,
								walletAddress,
								tradesFound: 0,
								ordersDetected: 0,
								success: true,
								note: 'No orders found in input data',
							},
						});
						continue;
					}

					const tradingHistory = await DEXTrade.parseTradingHistory(processableOrderHistory, walletAddress, blockfrost);

					if (tradingHistory.trades && tradingHistory.trades.length > 0) {
						for (const trade of tradingHistory.trades) {
							returnData.push({
								json: {
									operation: 'parseTradingHistory',
									network,
									...trade,
									success: true,
								},
							});
						}
					} else {
						returnData.push({
							json: {
								operation: 'parseTradingHistory',
								network,
								walletAddress,
								tradesFound: 0,
								transactionsScanned: processableOrderHistory.transactionsScanned || 0,
								ordersDetected: processableOrderHistory.totalOrders || 0,
								success: true,
								note: 'No completed trades found in the provided orders',
							},
						});
					}

				} catch (error: any) {
					const errorMessage = error instanceof Error ? error.message : 'Unknown error occurred';
					if (this.continueOnFail()) {
						returnData.push({
							json: {
								operation: 'parseTradingHistory',
								success: false,
								error: errorMessage,
								statusCode: error?.status_code || null,
							},
						});
					} else {
						throw new NodeOperationError(this.getNode(), `DEX trade parsing failed: ${errorMessage}`);
					}
				}
			}
		}
		return [returnData];
	}

	private static async parseTradingHistory(orderHistory: any, walletAddress: string, blockfrost?: BlockFrostAPI) {
		const orders = [...(orderHistory?.orders || [])].sort((a: any, b: any) =>
			new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime()
		);

		const pending: any[] = [];
		const trades: any[] = [];
		const processedOrders = new Set<string>();

		console.log(`Processing ${orders.length} orders for wallet ${walletAddress}`);

		for (const order of orders) {
			if (processedOrders.has(order.txHash)) continue;

			const { msg, extra } = DEXTrade.parseCIP674(order);
			const lowerMsg = msg.map((s: any) => String(s).toLowerCase());

			const isAgg = lowerMsg.some((s: string) => s.includes('aggregator market order'));
			const isExec = lowerMsg.some((s: string) => s.includes('order executed'));
			const isDexhunterTrade = lowerMsg.some((s: string) => s.includes('dexhunter trade'));
			const isSteelSwap = lowerMsg.some((s: string) => s.includes('steelswap'));
			const isOrderPlacement = (isDexhunterTrade || isSteelSwap) && !isExec && !isAgg;

			console.log(`Order ${order.txHash}: isAgg=${isAgg}, isExec=${isExec}, isDexhunter=${isDexhunterTrade}, isPlacement=${isOrderPlacement}`);

			if (isAgg && extra) {
				console.log(`Processing aggregator order: ${order.txHash}`);
				const opt = DEXTrade.extractOrderOptions(extra);
				if (opt?.amountIn && opt?.assetIn && opt?.assetOut) {
					pending.push({
						ts: new Date(order.timestamp).getTime(),
						txid: order.txHash,
						assetIn: opt.assetIn,
						assetOut: opt.assetOut,
						amountIn: opt.amountIn,
						executions: [],
						isComplete: false,
						type: 'aggregator'
					});
				}
				continue;
			}

			if (isOrderPlacement) {
				console.log(`Processing order placement: ${order.txHash}`);
				const orderIntent = DEXTrade.extractOrderIntent(order, walletAddress);
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
						type: 'placement'
					});
				}
				continue;
			}

			if (isExec) {
				console.log(`Processing execution: ${order.txHash}`);
				const ts = new Date(order.timestamp).getTime();
				const candidates = pending
					.filter((p: any) => !p.isComplete && ts - p.ts >= 0 && ts - p.ts <= 10 * 60 * 1000)
					.sort((a: any, b: any) => b.ts - a.ts);

				if (candidates.length > 0) {
					const delivered = DEXTrade.tokensReceived(order, walletAddress, undefined);
					let matchingOrder = candidates[0];

					if (delivered) {
						const byUnit = candidates.find((p: any) =>
							DEXTrade.sameUnit(p.assetOut, delivered.unit)
						);
						if (byUnit) matchingOrder = byUnit;
					}

					matchingOrder.executions.push({
						txHash: order.txHash,
						platform: order.dexDetection?.platform || 'Unknown',
						timestamp: order.timestamp,
						delivered: delivered,
						order: order
					});

					const isComplete = DEXTrade.isOrderComplete(matchingOrder, ts);
					if (isComplete) {
						matchingOrder.isComplete = true;
						const trade = DEXTrade.processFinalTrade(matchingOrder);
						if (trade) {
							trades.push(trade);
						}
					}
				} else {
					const orphanedTrade = DEXTrade.processOrphanedExecution(order, walletAddress);
					if (orphanedTrade) {
						trades.push(orphanedTrade);
					}
				}
				processedOrders.add(order.txHash);
				continue;
			}

			if (!isAgg && !isExec && !isOrderPlacement) {
				const standaloneTrade = DEXTrade.processGenericDexTransaction(order, walletAddress);
				if (standaloneTrade) {
					trades.push(standaloneTrade);
				}
				processedOrders.add(order.txHash);
			}
		}

		// Process remaining incomplete orders
		const now = Date.now();
		for (const pendingOrder of pending) {
			if (!pendingOrder.isComplete) {
				if (pendingOrder.executions.length > 0) {
					const lastExecTime = Math.max(...pendingOrder.executions.map((e: any) =>
						new Date(e.timestamp).getTime()
					));

					if (now - lastExecTime > 5 * 60 * 1000) {
						const trade = DEXTrade.processFinalTrade(pendingOrder);
						if (trade) {
							trades.push(trade);
						}
					}
				} else {
					const orderAge = now - pendingOrder.ts;
					if (orderAge > 2 * 60 * 1000) {
						const standaloneTrade = await DEXTrade.processStandaloneTrade(
							pendingOrder,
							walletAddress,
							blockfrost
						);
						if (standaloneTrade) {
							trades.push(standaloneTrade);
						}
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
				swapTrades: trades.filter(t => t.direction === 'SWAP').length,
				ordersScanned: orders.length,
				transactionsScanned: orderHistory.transactionsScanned || 0,
				aggregatorTrades: trades.filter(t => t.executionCount > 1).length,
				multiStepTrades: trades.filter(t => t.deliveryTxHash).length,
			}
		};
	}

	private static async processStandaloneTrade(order: any, walletAddress: string, blockfrost?: BlockFrostAPI) {
		if (!blockfrost || order.orderType !== 'buy') {
			return DEXTrade.createIncompleteTradeRecord(order);
		}

		try {
			const orderTime = new Date(order.ts);
			const searchEnd = new Date(orderTime.getTime() + (2 * 60 * 60 * 1000));
			const recentTxs = await blockfrost.addressesTransactions(walletAddress, {
				count: 50,
				order: 'desc'
			});

			for (const txRef of recentTxs) {
				try {
					const txDetails = await blockfrost.txs(txRef.tx_hash);
					const txTime = new Date(txDetails.block_time * 1000);

					if (txTime <= orderTime || txTime > searchEnd) continue;

					const txUtxos = await blockfrost.txsUtxos(txRef.tx_hash);

					for (const output of txUtxos.outputs) {
						if (output.address === walletAddress) {
							for (const amount of output.amount) {
								if (amount.unit !== 'lovelace') {
									const adaSpent = Number(order.amountIn) / 1e6;
									const tokenAmount = Number(amount.quantity);
									const identifier = DEXTrade.normalizeIdentifier(amount.unit);
									const assetName = DEXTrade.decodeAssetNameFromIdentifier(identifier);
									const price = adaSpent > 0 && tokenAmount > 0 ? adaSpent / tokenAmount : 0;

									return {
										timestamp: new Date(order.ts).toISOString(),
										direction: "BUY",
										assetName,
										identifier,
										amountIn: DEXTrade.formatNum6(adaSpent),
										amountOut: DEXTrade.formatNum6(tokenAmount),
										price: DEXTrade.formatPrice6(price),
										txHash: order.txid,
										deliveryTxHash: txRef.tx_hash,
										executionCount: 2,
										platforms: 'Multi-step DEX',
										executionHashes: [order.txid, txRef.tx_hash],
										deliveryTimestamp: txTime.toISOString(),
									};
								}
							}
						}
					}
				} catch (error) {
					continue;
				}
				await new Promise(resolve => setTimeout(resolve, 50));
			}
		} catch (error) {
			console.log('Error searching for delivery:', error);
		}

		return DEXTrade.createIncompleteTradeRecord(order, 'Multi-step trade - delivery not found within 2-hour window');
	}

	private static createIncompleteTradeRecord(order: any, customNote?: string) {
		return {
			timestamp: new Date(order.ts).toISOString(),
			direction: order.orderType?.toUpperCase() || "UNKNOWN",
			assetName: order.assetOut !== 'unknown' ?
				DEXTrade.decodeAssetNameFromIdentifier(DEXTrade.normalizeIdentifier(order.assetOut)) :
				"Unknown Asset",
			identifier: order.assetOut || "unknown",
			amountIn: order.amountIn ? DEXTrade.formatNum6(Number(order.amountIn) / 1e6) : "0.000000",
			amountOut: "0.000000",
			price: "0.000000",
			txHash: order.txid,
			executionCount: 1,
			platforms: 'Multi-step DEX',
			executionHashes: [order.txid],
			note: customNote || "Multi-step trade detected but delivery transaction not found"
		};
	}

	private static extractOrderIntent(order: any, walletAddress: string) {
		const userInputs = order.inputs?.filter((input: any) => input.address === walletAddress) || [];
		const userOutputs = order.outputs?.filter((output: any) => output.address === walletAddress) || [];

		if (userInputs.length === 0) return null;

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

		if (tokensIn.length > 0 && tokensOut.length === 0) {
			const primaryToken = tokensIn[0];
			return {
				assetIn: primaryToken.unit,
				assetOut: 'lovelace',
				amountIn: primaryToken.quantity,
				orderType: 'sell'
			};
		} else if (tokensIn.length === 0 && adaIn > adaOut + 1000000) {
			const netAda = adaIn - adaOut;
			return {
				assetIn: 'lovelace',
				assetOut: 'unknown',
				amountIn: String(netAda),
				orderType: 'buy'
			};
		} else if (tokensIn.length > 0 && tokensOut.length > 0) {
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

	private static isOrderComplete(order: any, currentTime: number): boolean {
		if (order.executions.length === 0) return false;
		const lastExecTime = Math.max(...order.executions.map((e: any) =>
			new Date(e.timestamp).getTime()
		));
		return currentTime - lastExecTime > 2 * 60 * 1000;
	}

	private static processFinalTrade(order: any) {
		if (order.executions.length === 0) return null;

		const totalDelivered = order.executions.reduce((total: any, exec: any) => {
			if (exec.delivered) {
				if (!total.unit) {
					total.unit = exec.delivered.unit;
					total.qty = 0;
				}
				if (DEXTrade.sameUnit(total.unit, exec.delivered.unit)) {
					total.qty += exec.delivered.qty;
				}
			}
			return total;
		}, { unit: null, qty: 0 });

		const platforms = [...new Set(order.executions.map((e: any) => e.platform))];
		const firstExecution = order.executions[0];

		if (order.assetIn === 'lovelace' && totalDelivered.unit && totalDelivered.qty > 0) {
			const adaIn = DEXTrade.toAda(order.amountIn);
			const tokenOut = totalDelivered.qty;

			if (adaIn && Number.isFinite(tokenOut) && tokenOut > 0) {
				const identifier = DEXTrade.normalizeIdentifier(totalDelivered.unit);
				const assetName = DEXTrade.decodeAssetNameFromIdentifier(identifier);
				const rawPrice = adaIn / tokenOut;

				return {
					timestamp: firstExecution.timestamp,
					direction: "BUY",
					assetName,
					identifier,
					amountIn: DEXTrade.formatNum6(adaIn),
					amountOut: DEXTrade.formatNum6(tokenOut),
					price: DEXTrade.formatPrice6(rawPrice),
					txHash: order.txid,
					executionCount: order.executions.length,
					platforms: platforms.join(', '),
					executionHashes: order.executions.map((e: any) => e.txHash),
				};
			}
		}

		return null;
	}

	private static processOrphanedExecution(order: any, walletAddress: string) {
		const delivered = DEXTrade.tokensReceived(order, walletAddress, undefined);
		if (!delivered) return null;

		let adaSpent = 0;
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
			const adaIn = DEXTrade.toAda(String(adaSpent));
			const tokenOut = delivered.qty;

			if (adaIn && Number.isFinite(tokenOut) && tokenOut > 0) {
				const identifier = DEXTrade.normalizeIdentifier(delivered.unit);
				const assetName = DEXTrade.decodeAssetNameFromIdentifier(identifier);
				const rawPrice = adaIn / tokenOut;

				return {
					timestamp: order.timestamp,
					direction: "BUY",
					assetName,
					identifier,
					amountIn: DEXTrade.formatNum6(adaIn),
					amountOut: DEXTrade.formatNum6(tokenOut),
					price: DEXTrade.formatPrice6(rawPrice),
					txHash: order.txHash,
					executionCount: 1,
					platforms: order.dexDetection?.platform || 'Unknown',
					executionHashes: [order.txHash],
					note: "Orphaned execution - no matching placement order found"
				};
			}
		}

		return null;
	}

	private static processGenericDexTransaction(order: any, walletAddress: string) {
		const userInputs = order.inputs?.filter((input: any) => input.address === walletAddress) || [];
		const userOutputs = order.outputs?.filter((output: any) => output.address === walletAddress) || [];

		if (userInputs.length === 0) return null;

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

		if (tokensOut.length > 0) {
			const tokenReceived = tokensOut[0];
			const adaSpent = adaIn - adaOut;

			if (adaSpent > 0) {
				const adaAmount = DEXTrade.toAda(String(adaSpent));
				const tokenAmount = Number(tokenReceived.quantity);
				const identifier = DEXTrade.normalizeIdentifier(tokenReceived.unit);
				const assetName = DEXTrade.decodeAssetNameFromIdentifier(identifier);
				const price = adaAmount && tokenAmount > 0 ? adaAmount / tokenAmount : 0;

				return {
					timestamp: order.timestamp,
					direction: "BUY",
					assetName,
					identifier,
					amountIn: DEXTrade.formatNum6(adaAmount || 0),
					amountOut: DEXTrade.formatNum6(tokenAmount),
					price: DEXTrade.formatPrice6(price),
					txHash: order.txHash,
					executionCount: 1,
					platforms: order.dexDetection?.platform || 'Unknown DEX',
					executionHashes: [order.txHash],
				};
			}
		}

		const netAda = adaOut - adaIn;
		if (tokensIn.length > 0 && netAda > 1000000) {
			const tokenSent = tokensIn[0];
			const adaReceived = DEXTrade.toAda(String(netAda));
			const tokenAmount = Number(tokenSent.quantity);
			const identifier = DEXTrade.normalizeIdentifier(tokenSent.unit);
			const assetName = DEXTrade.decodeAssetNameFromIdentifier(identifier);
			const price = adaReceived && tokenAmount > 0 ? adaReceived / tokenAmount : 0;

			return {
				timestamp: order.timestamp,
				direction: "SELL",
				assetName,
				identifier,
				amountIn: DEXTrade.formatNum6(tokenAmount),
				amountOut: DEXTrade.formatNum6(adaReceived || 0),
				price: DEXTrade.formatPrice6(price),
				txHash: order.txHash,
				executionCount: 1,
				platforms: order.dexDetection?.platform || 'Unknown DEX',
				executionHashes: [order.txHash],
			};
		}

		return null;
	}

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

	private static sameUnit(u1: string, u2: string): boolean {
		return !!u1 && !!u2 && (u1 === u2 || u1.replace('.', '') === u2.replace('.', ''));
	}

	private static tokensReceived(order: any, walletAddr: string, expectedUnit?: string) {
		for (const o of (order.outputs || [])) {
			if (o?.address !== walletAddr) continue;
			for (const a of (o.amount || [])) {
				if (a.unit !== 'lovelace' && (!expectedUnit || DEXTrade.sameUnit(a.unit, expectedUnit))) {
					return { unit: a.unit, qty: Number(a.quantity) };
				}
			}
		}
		return null;
	}

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

	private static toAda(lovelaceStr: string): number | null {
		const n = Number(lovelaceStr);
		return Number.isFinite(n) ? n / 1e6 : null;
	}

	private static formatNum6(n: number): string {
		return Number(n).toFixed(6);
	}

	private static formatPrice6(p: number): string {
		return Number(p).toFixed(6);
	}
}