#!/usr/bin/perl

use JSON;
use Usergrid::Client;

my $json = JSON->new->allow_nonref;

# Create the client object that will be used for all subsequent requests
my $client = Usergrid::Client->new(
  organization => 'test-organization',
  application => 'test-app',
  api_url => 'http://localhost:8080/ROOT',
  trace => 0
);

# Create a test user
my %user = (username=>'testuser',password=>'1QAZ2wsx');
my $user_obj = $client->create("users", \%user);
print "Created test user\n";

# Log the test user in
$resp = $client->login('testuser', '1QAZ2wsx');
print "Logged in as test user.\n";

# Retrieve the user details by UUID
$resp = $client->retrieve_by_id("user", $resp->{'entities'}[0]->{'uuid'});
print "Retrieved user entity.\n";

$resp = $client->create("collection_foo", { name=> "bar", type=>"fruit" });
print "Created entity #1 - $resp->{'entities'}[0]->{'uuid'}\n";

$resp = $client->create("collection_foo", { name=> "baz", type=>"not-a-fruit" });
print "Created entity #2 - $resp->{'entities'}[0]->{'uuid'}\n";

$resp = $client->retrieve("collection_foo");

foreach $entity (@{$resp->{'entities'}}) {
  print "Retrieved $entity->{'name'} - $entity->{'uuid'}\n";
  $resp = $client->delete("collection_foo", $entity->{'uuid'});
}

# Get a management token
my $tok = $client->management_login('admin', 'admin');
print "Logged in as admin.\n";

# Delete the test user
$resp = $client->delete("users", $user_obj->{'entities'}[0]->{'uuid'});
print "Deleted the test user.\n";
