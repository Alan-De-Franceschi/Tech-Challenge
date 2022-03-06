SELECT COUNT(rental.customer_id) as rental_count, rental.customer_id, customer.first_name, customer.last_name, 
address.address, address.city, address.postal_code, address.latitude, address.longitude
FROM rental
INNER JOIN customer ON rental.customer_id = customer.customer_id
INNER JOIN address ON customer.address_id = address.address_id
GROUP BY rental.customer_id
ORDER BY rental_count DESC LIMIT 1;