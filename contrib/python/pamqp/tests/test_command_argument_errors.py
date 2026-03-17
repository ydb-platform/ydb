import unittest
import uuid

from pamqp import commands


class ArgumentErrorsTestCase(unittest.TestCase):

    def test_basic_consume_queue_length(self):
        with self.assertRaises(ValueError):
            commands.Basic.Consume(queue=str.ljust('A', 257))

    def test_basic_consume_queue_characters(self):
        with self.assertRaises(ValueError):
            commands.Basic.Consume(queue='*')

    def test_basic_consume_ticket(self):
        with self.assertRaises(ValueError):
            commands.Basic.Consume(ticket=46 & 2)  # Just ahead of me

    def test_basic_deliver_exchange_length(self):
        with self.assertRaises(ValueError):
            commands.Basic.Deliver('ctag0', 1, False, str.ljust('A', 128), 'k')

    def test_basic_deliver_exchange_characters(self):
        with self.assertRaises(ValueError):
            commands.Basic.Deliver('ctag0', 1, False, '*', 'k')

    def test_basic_get_queue_length(self):
        with self.assertRaises(ValueError):
            commands.Basic.Get(queue=str.ljust('A', 257))

    def test_basic_get_queue_characters(self):
        with self.assertRaises(ValueError):
            commands.Basic.Get(queue='*')

    def test_get_ticket(self):
        with self.assertRaises(ValueError):
            commands.Basic.Get(ticket=46 & 2)

    def test_basic_getempty_cluster_id(self):
        with self.assertRaises(ValueError):
            commands.Basic.GetEmpty(cluster_id='See my shadow changing')

    def test_basic_getok_exchange_length(self):
        with self.assertRaises(ValueError):
            commands.Basic.GetOk(1, False, str.ljust('A', 128), 'k', 0)

    def test_basic_getok_exchange_characters(self):
        with self.assertRaises(ValueError):
            commands.Basic.GetOk(1, False, '*', 'k', 0)

    def test_basic_properties_cluster_id(self):
        with self.assertRaises(ValueError):
            commands.Basic.Properties(cluster_id='See my shadow changing')

    def test_basic_publish_exchange_length(self):
        with self.assertRaises(ValueError):
            commands.Basic.Publish(exchange=str.ljust('A', 128))

    def test_basic_publish_exchange_characters(self):
        with self.assertRaises(ValueError):
            commands.Basic.Publish(exchange='*')

    def test_basic_publish_ticket(self):
        with self.assertRaises(ValueError):
            commands.Basic.Publish(ticket=46 & 2)

    def test_basic_return_exchange_length(self):
        with self.assertRaises(ValueError):
            commands.Basic.Return(404, 'Not Found', str.ljust('A', 128), 'k')

    def test_basic_return_exchange_characters(self):
        with self.assertRaises(ValueError):
            commands.Basic.Return(404, 'Not Found', '*', 'k')

    def test_connection_open_vhost(self):
        with self.assertRaises(ValueError):
            commands.Connection.Open(str.ljust('A', 128))

    def test_connection_open_capabilities(self):
        with self.assertRaises(ValueError):
            commands.Connection.Open(capabilities=str(uuid.uuid4()))

    def test_connection_open_insist(self):
        with self.assertRaises(ValueError):
            commands.Connection.Open(insist=True)

    def test_connection_openok_known_hosts(self):
        with self.assertRaises(ValueError):
            commands.Connection.OpenOk(known_hosts=str(uuid.uuid4()))

    def test_channel_open_out_of_band(self):
        with self.assertRaises(ValueError):
            commands.Channel.Open(out_of_band=str(uuid.uuid4()))

    def test_channel_openok_channel_id(self):
        with self.assertRaises(ValueError):
            commands.Channel.OpenOk(channel_id=str(uuid.uuid4()))

    def test_exchange_declare_ticket(self):
        with self.assertRaises(ValueError):
            commands.Exchange.Declare(ticket=46 & 2)  # Just ahead of me

    def test_exchange_declare_exchange_length(self):
        with self.assertRaises(ValueError):
            commands.Exchange.Declare(exchange=str.ljust('A', 128))

    def test_exchange_declare_exchange_characters(self):
        with self.assertRaises(ValueError):
            commands.Exchange.Declare(exchange='***')

    def test_exchange_delete_ticket(self):
        with self.assertRaises(ValueError):
            commands.Exchange.Delete(ticket=46 & 2)

    def test_exchange_delete_exchange_length(self):
        with self.assertRaises(ValueError):
            commands.Exchange.Delete(exchange=str.ljust('A', 128))

    def test_exchange_delete_exchange_characters(self):
        with self.assertRaises(ValueError):
            commands.Exchange.Delete(exchange='***')

    def test_exchange_bind_ticket(self):
        with self.assertRaises(ValueError):
            commands.Exchange.Bind(ticket=46 & 2)

    def test_exchange_bind_destination_length(self):
        with self.assertRaises(ValueError):
            commands.Exchange.Bind(destination=''.rjust(32768, '*'))

    def test_exchange_bind_destination_pattern(self):
        with self.assertRaises(ValueError):
            commands.Exchange.Bind(destination='Fortysix & 2')

    def test_exchange_bind_source_length(self):
        with self.assertRaises(ValueError):
            commands.Exchange.Bind(source=''.rjust(32768, '*'))

    def test_exchange_bind_source_pattern(self):
        with self.assertRaises(ValueError):
            commands.Exchange.Bind(source='Fortysix & 2')

    def test_exchange_unbind_ticket(self):
        with self.assertRaises(ValueError):
            commands.Exchange.Unbind(ticket=46 & 2)

    def test_exchange_unbind_destination_length(self):
        with self.assertRaises(ValueError):
            commands.Exchange.Unbind(destination=''.rjust(32768, '*'))

    def test_exchange_unbind_destination_pattern(self):
        with self.assertRaises(ValueError):
            commands.Exchange.Unbind(destination='Fortysix & 2')

    def test_exchange_unbind_source_length(self):
        with self.assertRaises(ValueError):
            commands.Exchange.Unbind(source=''.rjust(32768, '*'))

    def test_exchange_unbind_source_pattern(self):
        with self.assertRaises(ValueError):
            commands.Exchange.Unbind(source='Fortysix & 2')

    def test_queue_declare_ticket(self):
        with self.assertRaises(ValueError):
            commands.Queue.Declare(ticket=46 & 2)

    def test_queue_declare_queue_length(self):
        with self.assertRaises(ValueError):
            commands.Queue.Declare(queue=str.ljust('A', 257))

    def test_queue_declare_queue_characters(self):
        with self.assertRaises(ValueError):
            commands.Queue.Declare(queue='***')

    def test_queue_declare_queue_with_valid_name(self):
        self.assertIsNotNone(commands.Queue.Declare(queue='foo'))

    def test_queue_declareok_queue_length(self):
        with self.assertRaises(ValueError):
            commands.Queue.DeclareOk(str.ljust('A', 257), 0, 0)

    def test_queue_declareok_queue_characters(self):
        with self.assertRaises(ValueError):
            commands.Queue.DeclareOk('***', 0, 0)

    def test_queue_delete_ticket(self):
        with self.assertRaises(ValueError):
            commands.Queue.Delete(ticket=46 & 2)

    def test_queue_delete_queue_length(self):
        with self.assertRaises(ValueError):
            commands.Queue.Delete(queue=str.ljust('A', 257))

    def test_queue_delete_queue_characters(self):
        with self.assertRaises(ValueError):
            commands.Queue.Delete(queue='***')

    def test_queue_bind_ticket(self):
        with self.assertRaises(ValueError):
            commands.Queue.Bind(ticket=46 & 2)

    def test_queue_bind_queue_length(self):
        with self.assertRaises(ValueError):
            commands.Queue.Bind(queue=str.ljust('A', 257), exchange='B')

    def test_queue_bind_queue_characters(self):
        with self.assertRaises(ValueError):
            commands.Queue.Bind(queue='***', exchange='B')

    def test_queue_bind_exchange_length(self):
        with self.assertRaises(ValueError):
            commands.Queue.Bind(exchange=str.ljust('A', 128), queue='B')

    def test_queue_bind_exchange_characters(self):
        with self.assertRaises(ValueError):
            commands.Queue.Bind(exchange='***', queue='B')

    def test_queue_unbind_ticket(self):
        with self.assertRaises(ValueError):
            commands.Queue.Unbind(ticket=46 & 2)

    def test_queue_unbind_queue_length(self):
        with self.assertRaises(ValueError):
            commands.Queue.Unbind(queue=str.ljust('A', 257), exchange='B')

    def test_queue_unbind_queue_characters(self):
        with self.assertRaises(ValueError):
            commands.Queue.Unbind(queue='***', exchange='B')

    def test_queue_unbind_exchange_length(self):
        with self.assertRaises(ValueError):
            commands.Queue.Unbind(exchange=str.ljust('A', 128), queue='B')

    def test_queue_unbind_exchange_characters(self):
        with self.assertRaises(ValueError):
            commands.Queue.Unbind(exchange='***', queue='B')

    def test_queue_purge_ticket(self):
        with self.assertRaises(ValueError):
            commands.Queue.Purge(ticket=46 & 2)

    def test_queue_purge_queue_length(self):
        with self.assertRaises(ValueError):
            commands.Queue.Purge(queue=str.ljust('A', 257))

    def test_queue_purge_queue_characters(self):
        with self.assertRaises(ValueError):
            commands.Queue.Purge(queue='***')
