use std::sync::atomic::AtomicU8;

#[derive(Default)]
pub struct RwLock {
    val: AtomicU8
}

impl RwLock {
    pub fn w_lock() {
        
    }

    pub fn w_unlock() {

    }

    pub fn r_lock(){}

    pub fn r_unlock(){

    }


}