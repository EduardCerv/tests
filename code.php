<?php

namespace App\SomeClass;

use Doctrine\ORM\EntityManager;

class SomeClass
{
    private EntityManager $em;

    public function __construct(EntityManager $em)
    {
        $this->em = $em;
    }
    /*
        Some code here
    */

    /**
     * @param int $userId
     * @return array
     */
    public function getQueryForFieldConfiguration(int $userId): array
    {
        $qb = $this->em->createQueryBuilder();

        return $qb->select(
            "u.firsname, u.lastname, c.title, c.percent, c.description"
            )
            ->from('users', 'u')
            ->innerJoin('u', 'usercourses', 'uc', 'u.id = uc.userid')
            ->innerJoin('uc', 'courses', 'c', 'c.id = uc.courseid')
            ->where('u.id = :id')
            ->andWhere('uc.isvalid = true')
            ->andWhere('u.percent > 75')
            ->setParameter('id', $userId)
            ->getQuery()
            ->getResult();
    }
}