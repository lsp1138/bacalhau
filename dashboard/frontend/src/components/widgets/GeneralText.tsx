import React, { FC } from 'react'
import { styled } from '@mui/system'
import Typography from '@mui/material/Typography'

export const SmallText = styled('span')({
  fontSize: '0.8em',
})

export const SmallLink = styled('div')({
  fontSize: '0.8em',
  color: 'blue',
  cursor: 'pointer',
})

export const BoldSectionTitle: FC = ({
  children,
}) => {
  return (
    <Typography variant="subtitle1" sx={{
      fontWeight: 'bold',
    }}>
      { children }
    </Typography>
  )
}
