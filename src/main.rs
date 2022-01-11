use futures::stream::TryStreamExt;
use hex;
use hex_literal;

use mongodb::bson::{doc, DateTime};
use mongodb::{options::ClientOptions, Client};
use serde::{Deserialize, Serialize};
use std::error::Error;
use tokio;
use web3::contract::{Contract, Options};
#[derive(Debug, Serialize, Deserialize)]
struct Event {
    _id: String,
    block_hash: String,
    block_timestamp: DateTime,
    _updated_at: DateTime,
    transaction_hash: String,
    transaction_index: i64,
    address: String,
    log_index: i64,
    token0: String,
    token1: String,
    pair: String,
    _created_at: DateTime,
    block_number: i64,
    confirmed: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Parse a connection string into an options struct.
    let client_options = ClientOptions::parse("mongodb://161.35.205.127:56728").await?;
    let transport = web3::transports::Http::new(
        "https://speedy-nodes-nyc.moralis.io/68f6f29b17d640686d627f72/polygon/mainnet",
    )?;

    let web3 = web3::Web3::new(transport);
    // Get a handle to the deployment.
    let client = Client::with_options(client_options)?;

    let db = client.database("parse");
    for collection in db.list_collection_names(None).await? {
        let collection_data = db.collection::<Event>(&collection);
        println!("{}", collection_data.name());
    }

    let collection_data = db.collection::<Event>("QuickswapFactory");
    let mut cursor = collection_data.find(None, None).await?;

    while let Some(event) = cursor.try_next().await? {
        println!("Tx: {}", event.transaction_hash);
        println!("Token0: {}", event.token0);
        println!("Token1: {}", event.token1);
        let mut parsed = event.token0;
        parsed.remove(0);
        parsed.remove(0);
        let mut addressBytes = [0u8; 20];
        hex::decode_to_slice(parsed, &mut addressBytes as &mut [u8])?;

        let erc20 = Contract::from_json(
            web3.eth(),
            web3::types::H160(addressBytes),
            include_bytes!("../erc20.json"),
        )?;

        let result = erc20.query("name", (), None, Options::default(), None);
        let name0: String = result.await?;

        let mut parsed = event.token1;
        parsed.remove(0);
        parsed.remove(0);
        let mut addressBytes = [0u8; 20];
        hex::decode_to_slice(parsed, &mut addressBytes as &mut [u8])?;

        let erc20 = Contract::from_json(
            web3.eth(),
            web3::types::H160(addressBytes),
            include_bytes!("../erc20.json"),
        )?;

        let result = erc20.query("name", (), None, Options::default(), None);
        let name1: String = result.await?;

        println!("Pair: {} - {}", name0, name1)
    }
    Ok(())
}
