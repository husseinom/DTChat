use std::collections::HashMap;
use std::sync::{RwLock, Mutex};
use a_sabr::{
    node::Node,
    contact::Contact,
    node_manager::none::NoManagement,
    contact_manager::evl::EVLManager,
    contact_plan::from_ion_file::IONContactPlan,
    routing::Router,
    routing::aliases::build_generic_router,
    types::{NodeID, Date},
    bundle::Bundle,
    utils::pretty_print
};
use chrono::{DateTime, Timelike, Utc};
use std::io;

use crate::utils::socket::Endpoint;

pub struct PredictionConfig {
    ion_to_node_id : RwLock<HashMap<String,NodeID>>,
    router : Mutex<Box<dyn Router<NoManagement,EVLManager>+ Send + Sync>>,
    cp_start_time: Date

}

impl PredictionConfig {
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
        
        let cp_start_time = Utc::now().timestamp() as f64; // Use current time as contact plan start time
        Ok(PredictionConfig {
            ion_to_node_id: RwLock::new(ion_to_node_id),
            router : Mutex::new(router),
            cp_start_time
        })
    }

    pub fn get_node_id(&self,ion_id:&str) -> Option<NodeID>{
        self.ion_to_node_id.read().unwrap().get(ion_id).copied()
    }

    pub fn extract_ion_node_from_endpoint(endpoint: &Endpoint) -> Option<String> {
        match endpoint {
            Endpoint::Bp(bp_address) => {
                // Handle ipn: format (e.g., "ipn:10.1" -> "10")
                if bp_address.starts_with("ipn:") {
                    let after_ipn = &bp_address[4..]; // Remove "ipn:" prefix
                    if let Some(dot_pos) = after_ipn.find('.') {
                        return Some(after_ipn[..dot_pos].to_string());
                    } else {
                        // If no dot, return the whole number part
                        return Some(after_ipn.to_string());
                    }
                }
                if bp_address.chars().all(|c| c.is_ascii_digit()) {
                    return Some(bp_address.clone());
                }
                Some(bp_address.clone())
            }
            Endpoint::Udp(_) | Endpoint::Tcp(_) => {
                None
            }
        }
    }

    // Pas besoin, plutot enlever
    pub fn test_endpoint(&self, endpoint: &Endpoint) -> bool {
        if let Some(ion_id) = Self::extract_ion_node_from_endpoint(endpoint) {
            self.ion_to_node_id.read().unwrap().contains_key(&ion_id)
        } else {
            false
        }
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


    pub fn predict(&self, source_ion: &str, dest_ion: &str, message_size: f64) -> io::Result<Date> {

        println!("the data in the hashmap is : {:?}", self.ion_to_node_id.read().unwrap());

        println!(" Looking for route from '{}' to '{}'", source_ion, dest_ion);

        let source_node_id = self.get_node_id(source_ion).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Source ION ID '{}' not found in contact plan", source_ion)
            )
        })?;

        let dest_node_id = self.get_node_id(dest_ion).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Destination ION ID '{}' not found in contact plan", dest_ion)
            )
        })?;

        if source_node_id == dest_node_id {
            println!(" Source and destination are the same node ({}). Delivery time is 0.", source_node_id);
            return Ok(0.0);
        }


        println!("Found nodes: {} -> {}, {} -> {}", source_ion, source_node_id, dest_ion, dest_node_id);

        let bundle = Bundle {
            source: source_node_id,
            destinations: vec![dest_node_id],
            priority: 0,
            size: message_size,
            expiration: Date::MAX,
        };
        
        let cp_send_time = Utc::now().timestamp() as f64 - self.cp_start_time; // Calculate relative send time from contact plan start
    

        let excluded_nodes = vec![];

        let mut router = self.router.lock().unwrap();


        match router.route(bundle.source, &bundle, cp_send_time, &excluded_nodes) {
            Some(routing_output) => {
                // Only display the last element
                if let Some((_contact_ptr, (_contact, route_stages))) = routing_output.first_hops.iter().last() {
                    if let Some(last_stage) = route_stages.last() {
                        // verify that last_stage.to_node is the destination node
                        let last_stage_borrowed = last_stage.borrow();
    
                        if last_stage_borrowed.to_node != dest_node_id {
                            println!("  WARNING: Route validation failed!");
                            println!("   Expected destination: {} (ION {})", dest_node_id, dest_ion);
                            println!("   Last stage destination: {}", last_stage_borrowed.to_node);
        
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!("Route validation failed: last hop goes to node {} instead of expected destination {}", 
                                    last_stage_borrowed.to_node, dest_node_id)
                            ));
                        }

                        let delay = last_stage_borrowed.at_time;
                        let arrival_time = delay + self.cp_start_time;
                        return Ok(arrival_time);
                    }
                }
                Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Route found but no route stages available"
                ))
            }
            None => {
                println!("No route found from ION {} to ION {}", source_ion, dest_ion);
                Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("No route found from ION {} to ION {}", source_ion, dest_ion)
                ))
            }
        }
    }

}