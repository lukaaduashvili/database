//Constants used to work with raw pointers
const HEADER: u8 = 4;
const BTREE_PAGE_SIZE: u16 = 4096;
const BTREE_MAX_KEY_SIZE: u16 = 1000;
const BTREE_MAX_VAL_SIZE: u16 = 3000;

trait Tree {
    fn get(pointer: u64) -> BNode;
    fn new(node: BNode) -> u64;
    fn del(pointer: u64);
}

enum BNodeType {
    InternalNode,
    LeafNode,
}

impl BNodeType {
    fn from_u16(n: u16) -> BNodeType {
        match n {
            1 => BNodeType::InternalNode,
            2 => BNodeType::LeafNode,
            _ => unreachable!("Invalid value for BNodeType: {}", n),
        }
    }
}

struct BNode {
    /*raw data
    format:
    | type | n_keys |   pointers   |   offsets   | k-v pairs |
    |  2B  |   2B   |  n_keys * 8B | n_keys * 2B |  ....     |

    k-v pair format:
    | k_len | v_len | key | val |
    |   2B  |   2B  | ... | ... |
    */
    data: [u8; BTREE_PAGE_SIZE as usize],
}

impl BNode {
    //Return the type of current node
    fn b_type(&self) -> BNodeType {
        BNodeType::from_u16(u16::from_le_bytes(self.data[0..2].try_into().unwrap()))
    }

    //Returns the number of keys in current node
    fn n_keys(&self) -> u16 {
        u16::from_le_bytes(self.data[2..4].try_into().unwrap())
    }

    fn set_header(&mut self, b_type: u16, n_keys: u16) {
        let bytes = b_type.to_le_bytes();

        // Save type data
        // First two bytes correspond to node type

        //TODO this can be saved in 1 byte but not sure if it's worth implementing this optimization
        self.data[0..2].copy_from_slice(&bytes);

        let bytes = n_keys.to_le_bytes();

        //Save number of keys
        // 3rd and 4th bytes save the number of keys in node
        self.data[2..4].copy_from_slice(&bytes);
    }

    //Return the pointer for a child node corresponding to index idx
    fn get_ptr(&self, idx: u16) -> u64 {
        assert!(idx < self.n_keys());

        //Pointer positions start from offset of fixed size HEADER and are 8 bytes long
        let position: u16 = (HEADER) as u16 + 8 * idx;

        u64::from_le_bytes(
            self.data[position as usize..(position + 8) as usize]
                .try_into()
                .unwrap(),
        )
    }

    //Set pointer of child node referenced by idx
    fn set_ptr(&mut self, idx: u16, value: u64) {
        assert!(idx < self.n_keys());

        //Pointer positions start from offset of fixed size HEADER and are 8 bytes long
        let position: u16 = (HEADER) as u16 + 8 * idx;

        self.data[position as usize..(position + 8) as usize]
            .copy_from_slice(value.to_le_bytes().as_slice());
    }

    //Get the offset position for the key in data array based on key idx
    fn offset_position(&self, idx: u16) -> u16 {
        assert!(1 < idx && idx < self.n_keys());

        //Offset positions start after fixed header and pointers to the children
        //(idx - 1) is necessary since we do not explicitly store offset for the first key
        HEADER as u16 + 8 * self.n_keys() + 2 * (idx - 1)
    }

    //Get the key position in the data array based on offset
    fn get_offset(&self, idx: u16) -> u16 {
        if idx == 0 {
            return 0;
        }

        //Locate the offset position in data array
        let offset_position = self.offset_position(idx);

        //Use the position to return the actual offset value
        u16::from_le_bytes(
            self.data[offset_position as usize..(offset_position + 2) as usize]
                .try_into()
                .unwrap(),
        )
    }

    //Set the offset for a key at the offset position for idx
    fn set_offset(&mut self, idx: u16, value: u16) {
        //Locate the potential offset position in data array
        let offset_position = self.offset_position(idx);

        //Set the value at the located offset position
        self.data[offset_position as usize..(offset_position + 2) as usize]
            .copy_from_slice(value.to_le_bytes().as_slice());
    }

    //Get the position of kv pair in the data array
    fn get_kv_pair_position(&self, idx: u16) -> u16 {
        assert!(idx < self.n_keys());

        //Data starts for an offset of fixed Header + number of child pointers + number of key offsets
        HEADER as u16 + 8 * self.n_keys() + 2 * self.n_keys() + self.get_offset(idx)
    }

    //Get the pointer to data located at the key position
    fn get_key(&self, idx: u16) -> &[u8] {
        assert!(idx < self.n_keys());

        //Get the position of kv pair in array
        let position: u16 = self.get_kv_pair_position(idx);

        //Key length is stored in first two bytes of key data
        let key_length = u16::from_le_bytes(
            self.data[position as usize..(position + 2) as usize]
                .try_into()
                .unwrap(),
        );
        //Skip first 4 bytes key length and value length and return key length amount of bytes
        self.data[(position + 4) as usize..(position + 4 + key_length) as usize]
            .try_into()
            .unwrap()
    }

    //Get value for key which resides at index idx
    fn get_value(&self, idx: u16) -> &[u8] {
        assert!(idx < self.n_keys());

        //Get the position of kv pair in array
        let position: u16 = self.get_kv_pair_position(idx);

        //Key length is stored in first two bytes of kv data
        let key_length = u16::from_le_bytes(
            self.data[position as usize..(position + 2) as usize]
                .try_into()
                .unwrap(),
        );
        //Key length is stored in 3rd and 4th bytes of kv data
        let value_length = u16::from_le_bytes(
            self.data[(position + 2) as usize..(position + 4) as usize]
                .try_into()
                .unwrap(),
        );

        let position_of_value_data = position + 4 + key_length;

        self.data[position_of_value_data as usize..(position_of_value_data + value_length) as usize]
            .try_into()
            .unwrap()
    }

    fn num_used_bytes(&self) -> u16 {
        //Return the offset from the start of array to the end of last kv pair
        self.get_kv_pair_position(self.n_keys())
    }
}

pub struct BTree {
    root: u64,
}
