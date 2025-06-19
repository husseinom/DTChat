use std::collections::HashMap;
use std::sync::{RwLock, Mutex};
use chrono::{DateTime, Utc};
use a_sabr::{
    node::Node,
    contact::Contact,
    node_manager::none::NoManagement,
    contact_manager::evl::EVLManager,
    contact_plan::from_ion_file::IONContactPlan,
    routing::Router,
    routing::aliases::build_generic_router,
    types::NodeID,
    bundle::Bundle,
    utils::pretty_print
};
use std::io;

pub struct NetworkConfig {
    ion_to_node_id : RwLock<HashMap<String,NodeID>>,
    router : Mutex<Box<dyn Router<NoManagement,EVLManager>+ Send + Sync>>
}

impl NetworkConfig {
    pub fn new(contact_plan: &str) -> io::Result<Self> {
        let (nodes, contacts) = IONContactPlan::parse::<NoManagement, EVLManager>(contact_plan)?;

        let ion_to_node_id = Self::map_node_indices(contact_plan)?;

        // Generate the router
        let router = build_generic_router::<NoManagement, EVLManager>(
            "CgrFirstEndingContactGraph", // Use this instead of ContactGraph
            nodes,
            contacts,
            None
        );

        let router: Box<dyn Router<NoManagement, EVLManager> + Send + Sync> =
            unsafe { std::mem::transmute(router) };

        Ok(NetworkConfig {
            ion_to_node_id: RwLock::new(ion_to_node_id),
            router : Mutex::new(router),
        })
    }

    pub fn get_node_id(&self,ion_id:&str) -> Option<NodeID>{
        self.ion_to_node_id.read().unwrap().get(ion_id).copied()
    }


    pub fn map_node_indices(contact_plan: &str) -> io::Result<HashMap<String, NodeID>> {
        let (nodes, _contacts) = IONContactPlan::parse::<NoManagement, EVLManager>(contact_plan)?;
        let node_index_map: HashMap<String, NodeID> = nodes
            .iter()
            .enumerate()
            .map(|(index, node)| (node.get_node_name().to_string(), index as NodeID))
            .collect();
        Ok(node_index_map)
    }


    pub fn route_with_ion_ids(&self, source_ion: &str, dest_ion: &str, message_size: f64, send_time: DateTime<Utc>) -> Option<f64> {

        println!("the data in the hashmap is : {:?}", self.ion_to_node_id.read().unwrap());

        println!("🔍 Looking for route from '{}' to '{}'", source_ion, dest_ion);

        let source_node_id = self.get_node_id(source_ion)?;

        let dest_node_id = self.get_node_id(dest_ion)?;

        if source_node_id == dest_node_id {
            println!("⚠️  Source and destination are the same node ({}). Delivery time is 0.", source_node_id);
            return Some(0.0);
        }


        println!("✅ Found nodes: {} -> {}, {} -> {}", source_ion, source_node_id, dest_ion, dest_node_id);

        let bundle = Bundle {
            source: source_node_id,
            destinations: vec![dest_node_id],
            priority: 0,
            size: message_size,
            expiration: 10000.0,
        };

        let current_time = send_time.timestamp() as f64;  // Use actual send time
        println!("🔍 Current time (timestamp): {}", current_time);
        println!("🔍 Bundle: source={}, dest={:?}, size={}, expiration={}", 
            bundle.source, bundle.destinations, bundle.size, bundle.expiration);
        let excluded_nodes = vec![];

        let mut router = self.router.lock().unwrap();
        println!("🔍 Router locked successfully, calling route()...");
        match router.route(bundle.source, &bundle, current_time, &excluded_nodes) {
            Some(routing_output) => {
                println!("✅ Router returned Some(routing_output)!");
                println!("🔍 Number of first_hops: {}", routing_output.first_hops.len());
                println!("Route found from ION {} to ION {}!", source_ion, dest_ion);
                for (_contact_ptr, (contact, route_stages)) in &routing_output.first_hops {
                    let contact_borrowed: std::cell::Ref<'_, Contact<NoManagement, EVLManager>> = contact.as_ref().borrow();
                    println!("First hop: Contact {} -> {} (Start: {}, End: {})",
                        contact_borrowed.info.tx_node,
                        contact_borrowed.info.rx_node,
                        contact_borrowed.info.start,
                        contact_borrowed.info.end
                    );
                    for route_stage in route_stages {
                        let stage_borrowed = route_stage.as_ref().borrow();
                    
                        // Print the route stage info
                        pretty_print(route_stage.clone());
                    
                        // Check if this stage reaches our destination
                        if stage_borrowed.to_node == dest_node_id {
                            let arrival_time = stage_borrowed.at_time;
                            // let delivery_time_seconds = arrival_time ;
                        
                            println!("🎯 PBAT Result: Message will arrive at t={} ", 
                                arrival_time);
                        
                            return Some(arrival_time);
                        }
                    }
                }
            
            // If we get here, we didn't find a route to the destination
                println!("❌ No route found to destination node {}", dest_node_id);
                None
            }
            None => {
                println!("❌ No route found from node {} to node {}", bundle.source, bundle.destinations[0]);
                None
            }
        }
    }

}